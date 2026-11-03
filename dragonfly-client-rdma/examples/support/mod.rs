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

//! Diagnostic protocol. Integers are explicitly encoded; remote addresses are metadata.

use dragonfly_client_rdma::SessionError;

pub const FRAME_SIZE: usize = 48;
pub use dragonfly_client_rdma::MAX_TRANSFER_SIZE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    Request {
        length: u64,
        seed: u64,
    },
    Region {
        length: u64,
        address: u64,
        key: u64,
        seed: u64,
    },
    Verified {
        length: u64,
        checksum: u64,
    },
    Finished {
        length: u64,
        checksum: u64,
    },
}

impl Message {
    pub fn encode(self) -> [u8; FRAME_SIZE] {
        let (kind, words) = match self {
            Self::Request { length, seed } => (1u16, [length, seed, 0, 0, 0]),
            Self::Region {
                length,
                address,
                key,
                seed,
            } => (2, [length, address, key, seed, 0]),
            Self::Verified { length, checksum } => (3, [length, checksum, 0, 0, 0]),
            Self::Finished { length, checksum } => (4, [length, checksum, 0, 0, 0]),
        };
        let mut frame = [0; FRAME_SIZE];
        frame[..4].copy_from_slice(b"D7YR");
        frame[4..6].copy_from_slice(&1u16.to_be_bytes());
        frame[6..8].copy_from_slice(&kind.to_be_bytes());
        for (chunk, word) in frame[8..].as_chunks_mut::<8>().0.iter_mut().zip(words) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        frame
    }

    pub fn decode(frame: &[u8]) -> Result<Self, SessionError> {
        if frame.len() != FRAME_SIZE || &frame[..4] != b"D7YR" || frame[4..6] != [0, 1] {
            return Err(SessionError::Protocol(
                "invalid frame length, magic or version",
            ));
        }
        let chunks = frame[8..].as_chunks::<8>().0;
        let words: [u64; 5] = std::array::from_fn(|index| u64::from_be_bytes(chunks[index]));
        validate_length(words[0])?;
        let kind = u16::from_be_bytes(frame[6..8].try_into().unwrap());
        if words[4] != 0 || (kind != 2 && (words[2] != 0 || words[3] != 0)) {
            return Err(SessionError::Protocol("nonzero reserved field"));
        }
        match kind {
            1 => Ok(Self::Request {
                length: words[0],
                seed: words[1],
            }),
            2 => {
                words[1]
                    .checked_add(words[0])
                    .ok_or(SessionError::Protocol("remote address overflow"))?;
                Ok(Self::Region {
                    length: words[0],
                    address: words[1],
                    key: words[2],
                    seed: words[3],
                })
            }
            3 => Ok(Self::Verified {
                length: words[0],
                checksum: words[1],
            }),
            4 => Ok(Self::Finished {
                length: words[0],
                checksum: words[1],
            }),
            _ => Err(SessionError::Protocol("unknown message type")),
        }
    }
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
        let messages = [
            Message::Request {
                length: 4096,
                seed: 7,
            },
            Message::Region {
                length: 4096,
                address: 123,
                key: 99,
                seed: 7,
            },
            Message::Verified {
                length: 4096,
                checksum: 99,
            },
            Message::Finished {
                length: 4096,
                checksum: 99,
            },
        ];
        for message in messages {
            assert_eq!(Message::decode(&message.encode()).unwrap(), message);
        }
        assert!(Message::decode(&[]).is_err());
        let mut frame = messages[0].encode();
        frame[5] = 2;
        assert!(Message::decode(&frame).is_err());
        frame = messages[0].encode();
        frame[47] = 1;
        assert!(Message::decode(&frame).is_err());
        assert!(Message::decode(
            &Message::Region {
                length: 1,
                address: u64::MAX,
                key: 0,
                seed: 0
            }
            .encode()
        )
        .is_err());
        assert!(validate_length(0).is_err());
        assert!(validate_length(MAX_TRANSFER_SIZE as u64 + 1).is_err());
    }

    #[test]
    fn full_byte_comparison_detects_corruption() {
        let mut data: Vec<_> = (0..8193).map(|i| pattern(i, 42)).collect();
        verify(&data, 42).unwrap();
        let original = checksum(&data);
        data[8192] ^= 1;
        assert!(matches!(
            verify(&data, 42),
            Err(SessionError::Protocol("diagnostic byte comparison failed"))
        ));
        assert_ne!(checksum(&data), original);
    }
}
