// Copyright 2019 Fredrik Portström <https://portstrom.com>
// This is free software distributed under the terms specified in
// the file LICENSE at the top-level directory of this distribution.

use crate::case_folding_simple::CASE_FOLDING_SIMPLE;

// Patch: not in upstream. Upstream stores a word's last byte as a `Final` edge, so a word that is a
// prefix of another word cannot be stored (the second insertion is dropped or fails). Here every state
// can hold a payload and have outgoing edges, and `find` returns the longest word that matches.
struct State<T> {
	edges: Vec<(u8, u32)>,
	payload: Option<T>,
}

pub struct Trie<T> {
	states: Vec<State<T>>,
}

impl<T: Copy> Trie<T> {
	pub fn add_case_sensitive_term(&mut self, term: &str, payload: T) -> bool {
		self.add_term_internal(term, payload, false)
	}

	fn new_state(&mut self) -> u32 {
		self.states.push(State {
			edges: vec![],
			payload: None,
		});
		(self.states.len() - 1) as u32
	}

	fn edge(&self, from: u32, byte: u8) -> Option<u32> {
		self.states[from as usize]
			.edges
			.iter()
			.find(|(b, _)| *b == byte)
			.map(|(_, to)| *to)
	}

	/// Follows the bytes of `character` from `from`, creating states as needed. With `target`, the last
	/// byte leads to that state (when it has no edge yet), which makes a folded variant share the state
	/// of the original character.
	fn add_character(
		&mut self,
		from: u32,
		character: char,
		target: Option<u32>,
	) -> u32 {
		let mut buffer = [0; 4];
		let bytes = character.encode_utf8(&mut buffer).as_bytes();
		let mut state = from;
		for (index, byte) in bytes.iter().enumerate() {
			state = match self.edge(state, *byte) {
				Some(next) => next,
				None => {
					let next = match target {
						Some(target) if index + 1 == bytes.len() => target,
						_ => self.new_state(),
					};
					self.states[state as usize].edges.push((*byte, next));
					next
				}
			};
		}
		state
	}

	pub fn add_term(&mut self, term: &str, payload: T) -> bool {
		self.add_term_internal(term, payload, true)
	}

	fn add_term_internal(
		&mut self,
		term: &str,
		payload: T,
		case_folded: bool,
	) -> bool {
		let mut state = 0;
		for character in term.chars() {
			let before = state;
			state = self.add_character(before, character, None);
			if case_folded {
				if let Some(folded) = simple_fold(character) {
					for variant in folded {
						self.add_character(before, *variant, Some(state));
					}
				}
			}
		}
		if self.states[state as usize].payload.is_some() {
			return false;
		}
		self.states[state as usize].payload = Some(payload);
		true
	}

	/// The longest stored word that `text` starts with: `Ok((length, payload))`. Otherwise `Err` holds
	/// the position of the byte that ended the walk (`0` when the text ran out first).
	pub fn find(&self, text: &str) -> Result<(usize, T), usize> {
		let mut state = 0;
		let mut best = None;
		for (position, byte) in text.as_bytes().iter().cloned().enumerate() {
			match self.edge(state, byte) {
				Some(next) => {
					state = next;
					if let Some(payload) = self.states[state as usize].payload {
						best = Some((position + 1, payload));
					}
				}
				None => {
					return match best {
						Some(found) => Ok(found),
						None => Err(position),
					};
				}
			}
		}
		match best {
			Some(found) => Ok(found),
			None => Err(0),
		}
	}

	pub fn new() -> Self {
		let mut trie = Trie { states: vec![] };
		trie.new_state();
		trie
	}
}

fn simple_fold(character: char) -> Option<&'static [char]> {
	match CASE_FOLDING_SIMPLE
		.binary_search_by_key(&character, |&(character, _)| character)
	{
		Err(_) => None,
		Ok(index) => Some(CASE_FOLDING_SIMPLE[index].1),
	}
}

#[cfg(test)]
mod tests {
	use super::Trie;

	fn check(words: &[&str]) {
		let mut trie = Trie::new();
		for (i, w) in words.iter().enumerate() {
			assert!(trie.add_term(w, i as u32), "{w}");
		}
		let at = |t: &str| trie.find(t).map(|(n, p)| (n, words[p as usize]));
		assert_eq!(at("огл__"), Ok((6, "ОГЛ")));
		assert_eq!(at("ОГЛАВЛЕНИЕ__"), Ok((20, "ОГЛАВЛЕНИЕ")));
		// A longer candidate that fails partway falls back to the last word seen on the path.
		assert_eq!(at("оглавл__"), Ok((6, "ОГЛ")));
		assert!(trie.find("ОГ__").is_err());
		assert!(trie.find("").is_err());
	}

	#[test]
	fn prefix_word_short_first() {
		check(&["ОГЛ", "ОГЛАВЛЕНИЕ"]);
	}

	#[test]
	fn prefix_word_long_first() {
		check(&["ОГЛАВЛЕНИЕ", "ОГЛ"]);
	}

	#[test]
	fn duplicate_word_is_rejected() {
		let mut trie = Trie::new();
		assert!(trie.add_term("ab", 1u32));
		assert!(!trie.add_term("AB", 2));
		assert_eq!(trie.find("ab"), Ok((2, 1)));
	}
}
