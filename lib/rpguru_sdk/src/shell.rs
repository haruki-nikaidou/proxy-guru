//! Remote shell: transcript arithmetic and the worker → dashboard event mapping.
//!
//! The worker protocol (`guru.orchestration.agent`) and the dashboard API
//! (`guru.orchestration`) each define a `ShellEvent`. They share the transcript
//! model — `offset` is where an event starts, output takes one position per
//! byte, `started` and `finished` one each, everything else none — so the
//! master relays one as the other without reading more than it routes by.

use crate::orchestration as dashboard;
use crate::orchestration_agent as agent;

impl agent::ShellEvent {
    /// The position the event after this one starts at: where a watch resumes.
    pub fn end_offset(&self) -> u64 {
        use agent::shell_event::Event;
        let length = match &self.event {
            Some(Event::Output(output)) => u64::try_from(output.data.len()).unwrap_or(u64::MAX),
            Some(Event::Started(_) | Event::Finished(_)) => 1,
            _ => 0,
        };
        self.offset.saturating_add(length)
    }
}

impl dashboard::ShellEvent {
    /// The position the event after this one starts at: what a client
    /// reconnects with to resume without a gap or a repeat.
    pub fn end_offset(&self) -> u64 {
        use dashboard::shell_event::Event;
        let length = match &self.event {
            Some(Event::Output(output)) => u64::try_from(output.data.len()).unwrap_or(u64::MAX),
            Some(Event::Started(_) | Event::Finished(_)) => 1,
            _ => 0,
        };
        self.offset.saturating_add(length)
    }

    /// The dashboard's form of a worker's event. `None` for an `error`, which
    /// ends a watch rather than travelling as an event, and for an event with
    /// no body.
    pub fn from_agent(event: agent::ShellEvent) -> Option<Self> {
        use agent::shell_event::Event as W;
        use dashboard::shell_event::Event as D;
        let body = match event.event? {
            W::Output(output) => D::Output(dashboard::ShellOutput {
                stream: dashboard::ShellStream::from(
                    agent::ShellStream::try_from(output.stream)
                        .unwrap_or(agent::ShellStream::Unspecified),
                )
                .into(),
                data: output.data,
            }),
            W::Started(started) => D::Started(dashboard::ShellCommandStarted {
                command: started.command,
            }),
            W::Finished(finished) => D::Finished(dashboard::ShellCommandFinished {
                exit_code: finished.exit_code,
            }),
            W::Truncated(truncated) => D::Truncated(dashboard::ShellTruncated {
                dropped: truncated.dropped,
            }),
            W::Closed(closed) => D::Closed(dashboard::ShellSessionClosed {
                reason: dashboard::ShellCloseReason::from(
                    agent::ShellCloseReason::try_from(closed.reason)
                        .unwrap_or(agent::ShellCloseReason::Unspecified),
                )
                .into(),
            }),
            W::KeepAlive(_) => D::KeepAlive(dashboard::KeepAlive {}),
            W::Error(_) => return None,
        };
        Some(Self {
            offset: event.offset,
            event: Some(body),
        })
    }
}

impl From<agent::ShellStream> for dashboard::ShellStream {
    fn from(stream: agent::ShellStream) -> Self {
        match stream {
            agent::ShellStream::Unspecified => Self::Unspecified,
            agent::ShellStream::Stdout => Self::Stdout,
            agent::ShellStream::Stderr => Self::Stderr,
        }
    }
}

impl From<agent::ShellCloseReason> for dashboard::ShellCloseReason {
    fn from(reason: agent::ShellCloseReason) -> Self {
        match reason {
            agent::ShellCloseReason::Unspecified => Self::Unspecified,
            agent::ShellCloseReason::Closed => Self::Closed,
            agent::ShellCloseReason::Idle => Self::Idle,
            agent::ShellCloseReason::Exited => Self::Exited,
            agent::ShellCloseReason::Shutdown => Self::Shutdown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_follow_the_transcript_model() {
        let output = agent::ShellEvent {
            offset: 10,
            event: Some(agent::shell_event::Event::Output(agent::ShellOutput {
                stream: agent::ShellStream::Stderr.into(),
                data: b"abc".to_vec(),
            })),
        };
        assert_eq!(output.end_offset(), 13);
        let finished = agent::ShellEvent {
            offset: 13,
            event: Some(agent::shell_event::Event::Finished(
                agent::ShellCommandFinished { exit_code: 2 },
            )),
        };
        assert_eq!(finished.end_offset(), 14);
        let truncated = agent::ShellEvent {
            offset: 20,
            event: Some(agent::shell_event::Event::Truncated(
                agent::ShellTruncated { dropped: 5 },
            )),
        };
        assert_eq!(truncated.end_offset(), 20);

        let relayed = dashboard::ShellEvent::from_agent(output);
        assert_eq!(
            relayed,
            Some(dashboard::ShellEvent {
                offset: 10,
                event: Some(dashboard::shell_event::Event::Output(
                    dashboard::ShellOutput {
                        stream: dashboard::ShellStream::Stderr.into(),
                        data: b"abc".to_vec(),
                    }
                )),
            })
        );
        assert_eq!(relayed.map(|event| event.end_offset()), Some(13));
    }

    #[test]
    fn an_error_is_not_an_event() {
        let error = agent::ShellEvent {
            offset: 0,
            event: Some(agent::shell_event::Event::Error(agent::ShellError {
                code: agent::ShellErrorCode::NotFound.into(),
                message: "gone".into(),
            })),
        };
        assert_eq!(dashboard::ShellEvent::from_agent(error), None);
    }
}
