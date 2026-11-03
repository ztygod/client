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

//! Bounded adaptive polling: spin briefly, yield, then back off when idle.
use crate::SessionError;
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct Progress {
    idle: u32,
}
impl Progress {
    pub(super) fn wait(
        &mut self,
        deadline: Instant,
        operation: &'static str,
    ) -> Result<(), SessionError> {
        let now = Instant::now();
        if now >= deadline {
            return Err(SessionError::Timeout(operation));
        }
        self.idle = self.idle.saturating_add(1);
        match self.idle {
            1..=16 => std::hint::spin_loop(),
            17..=32 => std::thread::yield_now(),
            _ => {
                let micros = (1u64 << (self.idle - 33).min(6)).min(50);
                std::thread::sleep(Duration::from_micros(micros).min(deadline - now));
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_expired_budget_never_sleeps_or_resets_the_deadline() {
        let mut progress = Progress::default();
        assert!(matches!(
            progress.wait(Instant::now(), "poll"),
            Err(SessionError::Timeout("poll"))
        ));
        progress.idle = u32::MAX;
        progress
            .wait(Instant::now() + Duration::from_secs(1), "poll")
            .unwrap();
        assert_eq!(progress.idle, u32::MAX);
    }
}
