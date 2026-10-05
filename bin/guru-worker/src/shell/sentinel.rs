//! Finds the end-of-command sentinels the worker appends to every command and
//! strips them from the shell's output.
//!
//! stdout carries `\n<NONCE> <exit code>\n`, stderr `\n<NONCE>\n`. The nonce is
//! random per session, so output that merely looks like a sentinel (another
//! nonce, or the right one followed by anything else) passes through untouched.
//! A chunk that ends in what may be the start of a sentinel holds that suffix back
//! until the next chunk decides it.

/// What a chunk of a stream turns into, in order.
#[derive(Debug, PartialEq, Eq)]
pub enum Piece<'a> {
    Data(&'a [u8]),
    /// A sentinel; stdout's carries the command's exit code.
    Sentinel(Option<i32>),
}

pub struct Scanner {
    /// `\n` followed by the nonce.
    marker: Vec<u8>,
    /// stdout: ` <code>\n` follows the marker; stderr: `\n`.
    with_code: bool,
    /// The undecided tail of the last chunk.
    held: Vec<u8>,
}

enum Match {
    Complete {
        len: usize,
        code: Option<i32>,
    },
    /// The bytes so far are a sentinel's prefix.
    Partial,
    No,
}

/// `$?` is at most 255.
const MAX_CODE_DIGITS: usize = 3;

impl Scanner {
    pub fn new(nonce: &str, with_code: bool) -> Self {
        let mut marker = Vec::with_capacity(nonce.len().saturating_add(1));
        marker.push(b'\n');
        marker.extend_from_slice(nonce.as_bytes());
        Self {
            marker,
            with_code,
            held: Vec::new(),
        }
    }

    /// Splits `chunk` (after whatever was held back) into data and sentinels.
    pub fn feed(&mut self, chunk: &[u8], emit: &mut impl FnMut(Piece<'_>)) {
        if self.held.is_empty() {
            self.scan(chunk, emit);
        } else {
            let mut buf = std::mem::take(&mut self.held);
            buf.extend_from_slice(chunk);
            self.scan(&buf, emit);
        }
    }

    /// The stream ended: what was held back was output after all.
    pub fn finish(&mut self, emit: &mut impl FnMut(Piece<'_>)) {
        if !self.held.is_empty() {
            emit(Piece::Data(&self.held));
            self.held.clear();
        }
    }

    fn scan(&mut self, buf: &[u8], emit: &mut impl FnMut(Piece<'_>)) {
        let mut start = 0;
        let mut search = 0;
        while let Some(found) = buf
            .get(search..)
            .and_then(|rest| rest.iter().position(|b| *b == b'\n'))
        {
            let at = search.saturating_add(found);
            let candidate = buf.get(at..).unwrap_or_default();
            match self.match_at(candidate) {
                Match::Complete { len, code } => {
                    emit_data(buf, start, at, emit);
                    emit(Piece::Sentinel(code));
                    start = at.saturating_add(len);
                    search = start;
                }
                Match::Partial => {
                    emit_data(buf, start, at, emit);
                    self.held = candidate.to_vec();
                    return;
                }
                Match::No => search = at.saturating_add(1),
            }
        }
        emit_data(buf, start, buf.len(), emit);
    }

    /// Whether `rest`, which starts with `\n`, starts with a sentinel.
    fn match_at(&self, rest: &[u8]) -> Match {
        let marker = self.marker.as_slice();
        let common = rest.len().min(marker.len());
        if rest.get(..common) != marker.get(..common) {
            return Match::No;
        }
        let mut i = marker.len();
        if rest.len() < i {
            return Match::Partial;
        }
        if !self.with_code {
            return match rest.get(i) {
                None => Match::Partial,
                Some(b'\n') => Match::Complete {
                    len: i.saturating_add(1),
                    code: None,
                },
                Some(_) => Match::No,
            };
        }
        match rest.get(i) {
            None => return Match::Partial,
            Some(b' ') => i = i.saturating_add(1),
            Some(_) => return Match::No,
        }
        let digits = i;
        loop {
            match rest.get(i) {
                None => return Match::Partial,
                Some(b'0'..=b'9') if i.saturating_sub(digits) < MAX_CODE_DIGITS => {
                    i = i.saturating_add(1);
                }
                Some(b'\n') if i > digits => {
                    let code = rest
                        .get(digits..i)
                        .and_then(|d| std::str::from_utf8(d).ok())
                        .and_then(|d| d.parse().ok());
                    return match code {
                        Some(code) => Match::Complete {
                            len: i.saturating_add(1),
                            code: Some(code),
                        },
                        None => Match::No,
                    };
                }
                Some(_) => return Match::No,
            }
        }
    }
}

fn emit_data(buf: &[u8], from: usize, to: usize, emit: &mut impl FnMut(Piece<'_>)) {
    if let Some(data) = buf.get(from..to).filter(|d| !d.is_empty()) {
        emit(Piece::Data(data));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONCE: &str = "0123abcd";

    #[derive(Debug, PartialEq, Eq)]
    enum Owned {
        Data(Vec<u8>),
        Sentinel(Option<i32>),
    }

    /// Feeds `chunks` in order and returns what came out, adjacent data merged.
    fn run(with_code: bool, chunks: &[&[u8]], eof: bool) -> Vec<Owned> {
        let mut scanner = Scanner::new(NONCE, with_code);
        let mut out: Vec<Owned> = Vec::new();
        let mut emit = |piece: Piece<'_>| match (piece, out.last_mut()) {
            (Piece::Data(d), Some(Owned::Data(last))) => last.extend_from_slice(d),
            (Piece::Data(d), _) => out.push(Owned::Data(d.to_vec())),
            (Piece::Sentinel(code), _) => out.push(Owned::Sentinel(code)),
        };
        for chunk in chunks {
            scanner.feed(chunk, &mut emit);
        }
        if eof {
            scanner.finish(&mut emit);
        }
        out
    }

    fn data(d: &[u8]) -> Owned {
        Owned::Data(d.to_vec())
    }

    #[test]
    fn a_sentinel_is_stripped_with_the_newline_it_starts_with() {
        assert_eq!(
            run(true, &[b"/tmp\n\n0123abcd 0\n"], false),
            vec![data(b"/tmp\n"), Owned::Sentinel(Some(0))]
        );
        assert_eq!(
            run(false, &[b"warn\n\n0123abcd\n"], false),
            vec![data(b"warn\n"), Owned::Sentinel(None)]
        );
    }

    #[test]
    fn output_without_a_trailing_newline_keeps_every_byte() {
        assert_eq!(
            run(true, &[b"no newline\n0123abcd 127\nnext"], true),
            vec![
                data(b"no newline"),
                Owned::Sentinel(Some(127)),
                data(b"next")
            ]
        );
        // A sentinel right at the start: the command printed nothing.
        assert_eq!(
            run(true, &[b"\n0123abcd 1\n"], false),
            vec![Owned::Sentinel(Some(1))]
        );
    }

    #[test]
    fn a_sentinel_split_across_chunks_is_found() {
        let whole: &[u8] = b"out\n\n0123abcd 42\nafter";
        for split in 1..whole.len() {
            let (a, b) = whole.split_at(split);
            assert_eq!(
                run(true, &[a, b], true),
                vec![data(b"out\n"), Owned::Sentinel(Some(42)), data(b"after")],
                "split at {split}"
            );
        }
        // Byte by byte, on stderr.
        let whole: &[u8] = b"e\n0123abcd\n";
        let bytes: Vec<&[u8]> = whole.chunks(1).collect();
        assert_eq!(
            run(false, &bytes, true),
            vec![data(b"e"), Owned::Sentinel(None)]
        );
    }

    #[test]
    fn a_held_prefix_that_turns_out_not_to_be_a_sentinel_is_output() {
        assert_eq!(
            run(true, &[b"a\n0123", b"xyz\n"], true),
            vec![data(b"a\n0123xyz\n")]
        );
        // Still undecided when the stream ends.
        assert_eq!(run(true, &[b"a\n0123ab"], true), vec![data(b"a\n0123ab")]);
        // Undecided bytes are not output yet.
        assert_eq!(run(true, &[b"a\n0123ab"], false), vec![data(b"a")]);
    }

    #[test]
    fn a_fake_nonce_or_a_malformed_sentinel_passes_through() {
        for fake in [
            &b"\nffffffff 0\n"[..],
            b"\n0123abcd\n",
            b"\n0123abcd x\n",
            b"\n0123abcd 1234\n",
            b"\n0123abcd \n",
            b"\n0123abcdd 0\n",
        ] {
            assert_eq!(run(true, &[fake], true), vec![data(fake)], "{fake:?}");
        }
        assert_eq!(
            run(false, &[b"\n0123abcd 0\n"], true),
            vec![data(b"\n0123abcd 0\n")]
        );
        // The real one after a fake one is still found.
        assert_eq!(
            run(true, &[b"\nffff 0\n\n0123abcd 3\n"], false),
            vec![data(b"\nffff 0\n"), Owned::Sentinel(Some(3))]
        );
    }
}
