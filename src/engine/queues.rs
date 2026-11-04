//! Which queue to try first on each dequeue.

use rand::seq::SliceRandom;

#[derive(Debug, Clone)]
pub struct QueueOrder {
    weights: Vec<(String, u32)>,
    strict: Option<Vec<String>>,
}

impl QueueOrder {
    /// Weights are normalized by their GCD, as in asynq, so {a: 6, b: 3}
    /// behaves like {a: 2, b: 1}.
    pub fn new(queues: &[(String, u32)], strict_priority: bool) -> Self {
        let mut weights: Vec<(String, u32)> =
            queues.iter().filter(|(_, w)| *w > 0).cloned().collect();
        let g = weights.iter().fold(0, |acc, (_, w)| gcd(acc, *w)).max(1);
        for (_, w) in &mut weights {
            *w /= g;
        }
        let strict = strict_priority.then(|| {
            let mut sorted = weights.clone();
            sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            sorted.into_iter().map(|(q, _)| q).collect()
        });
        Self { weights, strict }
    }

    pub fn names(&self) -> Vec<String> {
        self.weights.iter().map(|(q, _)| q.clone()).collect()
    }

    pub fn weights(&self) -> &[(String, u32)] {
        &self.weights
    }

    pub fn strict(&self) -> bool {
        self.strict.is_some()
    }

    /// Strict: always highest priority first. Weighted: each queue appears
    /// `weight` times in a shuffled list and the first occurrences win, so a
    /// queue with weight 6 is tried first ~6x as often as one with weight 1,
    /// but low-priority queues are never starved.
    pub fn order(&self) -> Vec<String> {
        if self.weights.len() <= 1 {
            return self.names();
        }
        if let Some(sorted) = &self.strict {
            return sorted.clone();
        }
        let mut expanded: Vec<&str> = self
            .weights
            .iter()
            .flat_map(|(q, w)| std::iter::repeat_n(q.as_str(), *w as usize))
            .collect();
        expanded.shuffle(&mut rand::rng());
        let mut out: Vec<String> = Vec::with_capacity(self.weights.len());
        for q in expanded {
            if !out.iter().any(|o| o == q) {
                out.push(q.to_string());
                if out.len() == self.weights.len() {
                    break;
                }
            }
        }
        out
    }
}

fn gcd(a: u32, b: u32) -> u32 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qs(v: &[(&str, u32)]) -> Vec<(String, u32)> {
        v.iter().map(|(q, w)| (q.to_string(), *w)).collect()
    }

    #[test]
    fn strict_sorts_by_priority() {
        let o = QueueOrder::new(&qs(&[("low", 1), ("critical", 6), ("default", 3)]), true);
        assert_eq!(o.order(), vec!["critical", "default", "low"]);
    }

    #[test]
    fn weighted_normalizes_and_roughly_respects_weights() {
        let o = QueueOrder::new(&qs(&[("a", 6), ("b", 3)]), false);
        assert_eq!(o.weights(), &qs(&[("a", 2), ("b", 1)])[..]);
        let firsts = (0..3000).filter(|_| o.order()[0] == "a").count();
        assert!(
            (1800..2200).contains(&firsts),
            "a first {firsts}/3000 times"
        );
        assert_eq!(o.order().len(), 2);
    }
}
