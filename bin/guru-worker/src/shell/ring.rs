//! A session's transcript: tagged records at monotonically increasing positions,
//! kept within a byte budget by dropping the oldest records first.
//!
//! Output occupies one position per byte, a command's start and finish one each.
//! Appending never waits on anyone: a reader that falls behind the oldest record
//! kept learns how many positions it lost from [`Entry::Truncated`].

use std::collections::VecDeque;

/// Bytes charged per record on top of its payload, so a flood of tiny records
/// cannot hide behind a budget that only counts payload.
pub const RECORD_OVERHEAD: usize = 32;
/// The largest output record: new output fills the newest record of its stream up
/// to this, then starts another. A quarter of the budget at most, so dropping
/// whole records keeps the transcript close to the budget.
const MAX_OUTPUT_RECORD: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Output { stream: Stream, data: Vec<u8> },
    Started { command: String },
    Finished { exit_code: i32 },
}

impl Record {
    /// The transcript positions it occupies.
    fn positions(&self) -> u64 {
        match self {
            Self::Output { data, .. } => u64::try_from(data.len()).unwrap_or(u64::MAX),
            Self::Started { .. } | Self::Finished { .. } => 1,
        }
    }

    /// What it costs against the budget.
    fn cost(&self) -> usize {
        let payload = match self {
            Self::Output { data, .. } => data.len(),
            Self::Started { command } => command.len(),
            Self::Finished { .. } => 0,
        };
        payload.saturating_add(RECORD_OVERHEAD)
    }
}

/// One step of a read, at the position it is paired with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// The positions from the reader's cursor up to here are gone.
    Truncated { dropped: u64 },
    /// A record, or the rest of an output record the cursor pointed into.
    Record(Record),
}

pub struct Ring {
    /// Each record with the position it starts at, oldest first.
    records: VecDeque<(u64, Record)>,
    /// The position the next record starts at.
    end: u64,
    used: usize,
    budget: usize,
    max_output: usize,
}

impl Ring {
    pub fn new(budget: usize) -> Self {
        Self {
            records: VecDeque::new(),
            end: 0,
            used: 0,
            budget,
            max_output: (budget / 4).clamp(1, MAX_OUTPUT_RECORD),
        }
    }

    /// The oldest position still held; [`Ring::end`] while nothing is.
    pub fn start(&self) -> u64 {
        self.records.front().map_or(self.end, |(offset, _)| *offset)
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    /// Appends a command's start or finish.
    pub fn push(&mut self, record: Record) {
        if let Record::Output { stream, data } = record {
            self.push_output(stream, &data);
            return;
        }
        self.append(record);
        self.evict();
    }

    /// Appends output, continuing the newest record when it is of the same stream.
    pub fn push_output(&mut self, stream: Stream, mut data: &[u8]) {
        while !data.is_empty() {
            let room = match self.records.back_mut() {
                Some((
                    _,
                    Record::Output {
                        stream: last,
                        data: held,
                    },
                )) if *last == stream && held.len() < self.max_output => {
                    let take = self.max_output.saturating_sub(held.len()).min(data.len());
                    let (head, rest) = data.split_at(take);
                    held.extend_from_slice(head);
                    self.used = self.used.saturating_add(take);
                    self.end = self
                        .end
                        .saturating_add(u64::try_from(take).unwrap_or(u64::MAX));
                    data = rest;
                    continue;
                }
                _ => self.max_output.min(data.len()),
            };
            let (head, rest) = data.split_at(room);
            self.append(Record::Output {
                stream,
                data: head.to_vec(),
            });
            data = rest;
        }
        self.evict();
    }

    fn append(&mut self, record: Record) {
        let offset = self.end;
        self.end = self.end.saturating_add(record.positions());
        self.used = self.used.saturating_add(record.cost());
        self.records.push_back((offset, record));
    }

    /// Drops the oldest records until the budget holds. The newest record always
    /// stays, even alone over budget (a command longer than the whole buffer).
    fn evict(&mut self) {
        while self.used > self.budget && self.records.len() > 1 {
            if let Some((_, record)) = self.records.pop_front() {
                self.used = self.used.saturating_sub(record.cost());
            }
        }
    }

    /// Appends to `out` what a reader at `cursor` has not seen yet, stopping once
    /// about `max_bytes` of output were copied, and returns the cursor after it.
    ///
    /// A cursor past the end reads from the end; one before the oldest position
    /// still held first gets [`Entry::Truncated`] for the positions it missed.
    pub fn read(&self, cursor: u64, max_bytes: usize, out: &mut Vec<(u64, Entry)>) -> u64 {
        let mut cursor = cursor.min(self.end);
        let start = self.start();
        if cursor < start {
            out.push((
                start,
                Entry::Truncated {
                    dropped: start.saturating_sub(cursor),
                },
            ));
            cursor = start;
        }
        // The record holding `cursor`: the last one starting at or before it.
        let first = self
            .records
            .partition_point(|(offset, _)| *offset <= cursor)
            .saturating_sub(1);
        let mut copied = 0usize;
        for (offset, record) in self.records.range(first..) {
            if copied >= max_bytes {
                break;
            }
            let end = offset.saturating_add(record.positions());
            if end <= cursor {
                continue;
            }
            let entry = match record {
                Record::Output { stream, data } => {
                    let skip =
                        usize::try_from(cursor.saturating_sub(*offset)).unwrap_or(usize::MAX);
                    let rest = data.get(skip..).unwrap_or_default();
                    copied = copied.saturating_add(rest.len());
                    Record::Output {
                        stream: *stream,
                        data: rest.to_vec(),
                    }
                }
                other => other.clone(),
            };
            out.push((cursor, Entry::Record(entry)));
            cursor = end;
        }
        cursor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(data: &[u8]) -> Entry {
        Entry::Record(Record::Output {
            stream: Stream::Stdout,
            data: data.to_vec(),
        })
    }

    fn read_all(ring: &Ring, cursor: u64) -> (Vec<(u64, Entry)>, u64) {
        let mut out = Vec::new();
        let next = ring.read(cursor, usize::MAX, &mut out);
        (out, next)
    }

    #[test]
    fn positions_count_output_bytes_and_one_per_command_mark() {
        let mut ring = Ring::new(1 << 20);
        ring.push(Record::Started {
            command: "pwd".into(),
        });
        ring.push_output(Stream::Stdout, b"/tmp\n");
        ring.push(Record::Finished { exit_code: 0 });
        assert_eq!((ring.start(), ring.end()), (0, 7));

        let (entries, next) = read_all(&ring, 0);
        assert_eq!(next, 7);
        assert_eq!(
            entries,
            vec![
                (
                    0,
                    Entry::Record(Record::Started {
                        command: "pwd".into()
                    })
                ),
                (1, output(b"/tmp\n")),
                (6, Entry::Record(Record::Finished { exit_code: 0 })),
            ]
        );
    }

    #[test]
    fn a_cursor_inside_an_output_record_reads_its_rest() {
        let mut ring = Ring::new(1 << 20);
        ring.push_output(Stream::Stdout, b"hello ");
        ring.push_output(Stream::Stdout, b"world");
        ring.push_output(Stream::Stderr, b"oops");
        let (entries, next) = read_all(&ring, 3);
        assert_eq!(next, 15);
        assert_eq!(
            entries,
            vec![
                (3, output(b"lo world")),
                (
                    11,
                    Entry::Record(Record::Output {
                        stream: Stream::Stderr,
                        data: b"oops".to_vec()
                    })
                ),
            ]
        );
        // Nothing new at the end, and a cursor beyond it reads from the end.
        assert_eq!(read_all(&ring, 15), (Vec::new(), 15));
        assert_eq!(read_all(&ring, 99), (Vec::new(), 15));
    }

    #[test]
    fn a_wrapped_ring_keeps_its_budget_and_reports_what_a_reader_missed() {
        let budget = 4096;
        let mut ring = Ring::new(budget);
        for _ in 0..100 {
            ring.push_output(Stream::Stdout, &[b'x'; 300]);
        }
        assert_eq!(ring.end(), 30_000);
        let start = ring.start();
        assert!(start > 0, "the oldest output was dropped");
        let held = usize::try_from(ring.end().saturating_sub(start)).unwrap_or(usize::MAX);
        assert!(
            held <= budget,
            "{held} bytes held over a {budget}-byte budget"
        );

        let (entries, next) = read_all(&ring, 10);
        assert_eq!(next, 30_000);
        assert_eq!(
            entries.first(),
            Some(&(
                start,
                Entry::Truncated {
                    dropped: start.saturating_sub(10)
                }
            ))
        );
        // The records after the marker are contiguous up to the end.
        let mut position = start;
        for (offset, entry) in entries.iter().skip(1) {
            assert_eq!(*offset, position);
            assert!(
                matches!(entry, Entry::Record(Record::Output { .. })),
                "unexpected entry {entry:?}"
            );
            if let Entry::Record(Record::Output { data, .. }) = entry {
                position = position.saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
            }
        }
        assert_eq!(position, 30_000);
        // A reader already past the oldest position misses nothing.
        let (entries, _) = read_all(&ring, start);
        assert!(!matches!(
            entries.first(),
            Some((_, Entry::Truncated { .. }))
        ));
    }

    #[test]
    fn the_newest_record_stays_even_over_budget() {
        let mut ring = Ring::new(4096);
        ring.push_output(Stream::Stdout, b"before");
        let command = "x".repeat(10_000);
        ring.push(Record::Started {
            command: command.clone(),
        });
        assert_eq!((ring.start(), ring.end()), (6, 7));
        let (entries, _) = read_all(&ring, 6);
        assert_eq!(
            entries,
            vec![(6, Entry::Record(Record::Started { command }))]
        );
    }

    #[test]
    fn a_read_stops_after_its_byte_limit_and_resumes_where_it_stopped() {
        let mut ring = Ring::new(1 << 20);
        for _ in 0..4 {
            ring.push_output(Stream::Stdout, &[b'a'; 8192]);
        }
        let mut out = Vec::new();
        let next = ring.read(0, 10_000, &mut out);
        assert_eq!(next, 16_384, "two records reach the limit");
        let mut rest = Vec::new();
        assert_eq!(ring.read(next, usize::MAX, &mut rest), 32_768);
        assert_eq!(rest.first().map(|(offset, _)| *offset), Some(16_384));
    }
}
