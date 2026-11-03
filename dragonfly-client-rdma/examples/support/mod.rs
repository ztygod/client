/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Version 2 diagnostic protocol. RELEASE is a trusted-peer promise that all
//! Reads completed and this export's address/key will never be used again.

pub use dragonfly_client_rdma::MAX_TRANSFER_SIZE;
use dragonfly_client_rdma::{ExportId, ExportedRegion, RemoteRegion, SessionError};
pub const FRAME_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    Request {
        length: u64,
        seed: u64,
    },
    Region {
        export: ExportedRegion,
        seed: u64,
    },
    Release {
        id: ExportId,
        length: u64,
        checksum: u64,
    },
    Released {
        id: ExportId,
        length: u64,
        checksum: u64,
    },
    Rotate,
    RotateAck,
    Done,
    Unavailable,
}
impl Message {
    pub fn encode(self) -> [u8; FRAME_SIZE] {
        let (kind, words) = match self {
            Self::Request { length, seed } => (1u16, [length, seed, 0, 0, 0, 0, 0]),
            Self::Region { export, seed } => (
                2,
                [
                    export.region.length,
                    export.region.address,
                    export.region.key,
                    seed,
                    export.id.session,
                    export.id.generation,
                    0,
                ],
            ),
            Self::Release {
                id,
                length,
                checksum,
            } => (3, [length, checksum, id.session, id.generation, 0, 0, 0]),
            Self::Released {
                id,
                length,
                checksum,
            } => (4, [length, checksum, id.session, id.generation, 0, 0, 0]),
            Self::Rotate => (5, [0; 7]),
            Self::RotateAck => (6, [0; 7]),
            Self::Done => (7, [0; 7]),
            Self::Unavailable => (8, [0; 7]),
        };
        let mut frame = [0; FRAME_SIZE];
        frame[..4].copy_from_slice(b"D7YR");
        frame[4..6].copy_from_slice(&2u16.to_be_bytes());
        frame[6..8].copy_from_slice(&kind.to_be_bytes());
        for (chunk, word) in frame[8..].as_chunks_mut::<8>().0.iter_mut().zip(words) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        frame
    }
    pub fn decode(frame: &[u8]) -> Result<Self, SessionError> {
        if frame.len() != FRAME_SIZE || &frame[..4] != b"D7YR" || frame[4..6] != [0, 2] {
            return Err(SessionError::Protocol(
                "invalid frame length, magic or version",
            ));
        }
        let chunks = frame[8..].as_chunks::<8>().0;
        let words: [u64; 7] = std::array::from_fn(|i| u64::from_be_bytes(chunks[i]));
        let kind = u16::from_be_bytes(frame[6..8].try_into().unwrap());
        let message = match kind {
            1 => {
                validate_length(words[0])?;
                Self::Request {
                    length: words[0],
                    seed: words[1],
                }
            }
            2 => {
                let region = RemoteRegion {
                    length: words[0],
                    address: words[1],
                    key: words[2],
                };
                region.validate(MAX_TRANSFER_SIZE)?;
                let id = checked_id(words[4], words[5])?;
                Self::Region {
                    export: ExportedRegion { id, region },
                    seed: words[3],
                }
            }
            3 | 4 => {
                validate_length(words[0])?;
                let id = checked_id(words[2], words[3])?;
                if kind == 3 {
                    Self::Release {
                        id,
                        length: words[0],
                        checksum: words[1],
                    }
                } else {
                    Self::Released {
                        id,
                        length: words[0],
                        checksum: words[1],
                    }
                }
            }
            5 => Self::Rotate,
            6 => Self::RotateAck,
            7 => Self::Done,
            8 => Self::Unavailable,
            _ => return Err(SessionError::Protocol("unknown message type")),
        };
        if message.encode() != frame {
            return Err(SessionError::Protocol("nonzero reserved field"));
        }
        Ok(message)
    }
}
fn checked_id(session: u64, generation: u64) -> Result<ExportId, SessionError> {
    if session == 0 || generation == 0 {
        return Err(SessionError::Protocol("invalid export identity"));
    }
    Ok(ExportId {
        session,
        generation,
    })
}

pub fn validate_length(length: u64) -> Result<usize, SessionError> {
    if length == 0 || length > MAX_TRANSFER_SIZE as u64 {
        return Err(SessionError::Protocol(
            "transfer length must be in 1..=64 MiB",
        ));
    }
    Ok(length as usize)
}

// A deterministic pattern lets the client compare every byte without sending a
// second copy over the message channel. It is test data, not a cryptographic hash.
pub fn pattern(index: usize, seed: u64) -> u8 {
    let mut value = (index as u64).wrapping_add(seed.wrapping_mul(0x9e3779b97f4a7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    (value ^ (value >> 31)) as u8
}

pub fn checksum(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

pub fn verify(data: &[u8], seed: u64) -> Result<(), SessionError> {
    for (index, actual) in data.iter().enumerate() {
        let expected = pattern(index, seed);
        if *actual != expected {
            return Err(SessionError::Protocol("diagnostic byte comparison failed"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wire_format_is_versioned_and_checks_remote_bounds() {
        let id = ExportId {
            session: 12,
            generation: 99,
        };
        let messages = [
            Message::Request {
                length: 4096,
                seed: 7,
            },
            Message::Region {
                export: ExportedRegion {
                    id,
                    region: RemoteRegion {
                        length: 4096,
                        address: 123,
                        key: 99,
                    },
                },
                seed: 7,
            },
            Message::Release {
                id,
                length: 4096,
                checksum: 99,
            },
            Message::Released {
                id,
                length: 4096,
                checksum: 99,
            },
            Message::Rotate,
            Message::RotateAck,
            Message::Done,
            Message::Unavailable,
        ];
        for message in messages {
            assert_eq!(Message::decode(&message.encode()).unwrap(), message);
        }
        assert!(Message::decode(&[]).is_err());
        for message in messages {
            let mut frame = message.encode();
            frame[5] = 1;
            assert!(Message::decode(&frame).is_err());
            frame = message.encode();
            frame[63] = 1;
            assert!(Message::decode(&frame).is_err());
        }
        let mut frame = messages[1].encode();
        frame[16..24].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(Message::decode(&frame).is_err());
        frame = messages[2].encode();
        frame[24..32].fill(0);
        assert!(Message::decode(&frame).is_err());
    }
    #[test]
    fn full_byte_comparison_detects_corruption() {
        let mut data: Vec<_> = (0..4096).map(|i| pattern(i, 42)).collect();
        verify(&data, 42).unwrap();
        data[123] ^= 1;
        assert!(verify(&data, 42).is_err());
    }
}
