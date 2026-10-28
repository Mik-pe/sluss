//! Bounded, conservative recognition of GitHub workflow dispatch triggers.
//! This does not validate the workflow's semantics, jobs or trustworthiness.
use std::collections::BTreeSet;
use yaml_rust2::{
    parser::{Event, Parser},
    scanner::TScalarStyle,
};

const MAX_BYTES: usize = 256 * 1024;
const MAX_EVENTS: usize = 50_000;
const MAX_DEPTH: usize = 64;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InvalidWorkflow;

/// Return whether the single YAML document explicitly declares the trigger.
/// Aliases outside `on` are ignored without expansion; an aliased trigger is
/// rejected because its value cannot be proven by this streaming inspection.
pub(crate) fn has_dispatch(bytes: &[u8]) -> Result<bool, InvalidWorkflow> {
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(InvalidWorkflow);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| InvalidWorkflow)?;
    let mut parser = Parser::new_from_str(text);
    let mut events = Vec::new();
    let mut depth = 0usize;
    loop {
        let (event, _) = parser.next_token().map_err(|_| InvalidWorkflow)?;
        match event {
            Event::MappingStart(..) | Event::SequenceStart(..) => {
                depth = depth.checked_add(1).ok_or(InvalidWorkflow)?;
                if depth > MAX_DEPTH {
                    return Err(InvalidWorkflow);
                }
            }
            Event::MappingEnd | Event::SequenceEnd => {
                depth = depth.checked_sub(1).ok_or(InvalidWorkflow)?;
            }
            _ => {}
        }
        let end = event == Event::StreamEnd;
        events.push(event);
        if events.len() > MAX_EVENTS {
            return Err(InvalidWorkflow);
        }
        if end {
            break;
        }
    }
    let mut cursor = Cursor { events, index: 0 };
    cursor.expect(Event::StreamStart)?;
    cursor.expect(Event::DocumentStart)?;
    if !matches!(cursor.take()?, Event::MappingStart(..)) {
        return Err(InvalidWorkflow);
    }
    let mut keys = BTreeSet::new();
    let mut found = false;
    while !matches!(cursor.peek()?, Event::MappingEnd) {
        let key = cursor.string_scalar()?;
        if !keys.insert(key.clone()) {
            return Err(InvalidWorkflow);
        }
        if key == "on" {
            found = cursor.triggers()?;
        } else {
            cursor.skip_node()?;
        }
    }
    cursor.expect(Event::MappingEnd)?;
    cursor.expect(Event::DocumentEnd)?;
    cursor.expect(Event::StreamEnd)?;
    Ok(found)
}

struct Cursor {
    events: Vec<Event>,
    index: usize,
}
impl Cursor {
    fn peek(&self) -> Result<&Event, InvalidWorkflow> {
        self.events.get(self.index).ok_or(InvalidWorkflow)
    }
    fn take(&mut self) -> Result<Event, InvalidWorkflow> {
        let event = self.peek()?.clone();
        self.index += 1;
        Ok(event)
    }
    fn expect(&mut self, expected: Event) -> Result<(), InvalidWorkflow> {
        if self.take()? == expected {
            Ok(())
        } else {
            Err(InvalidWorkflow)
        }
    }
    fn string_scalar(&mut self) -> Result<String, InvalidWorkflow> {
        match self.take()? {
            Event::Scalar(value, _, _, None) => Ok(value),
            _ => Err(InvalidWorkflow),
        }
    }
    fn skip_node(&mut self) -> Result<(), InvalidWorkflow> {
        match self.take()? {
            Event::Scalar(..) | Event::Alias(..) => Ok(()),
            Event::MappingStart(..) | Event::SequenceStart(..) => {
                let mut depth = 1usize;
                while depth > 0 {
                    match self.take()? {
                        Event::MappingStart(..) | Event::SequenceStart(..) => depth += 1,
                        Event::MappingEnd | Event::SequenceEnd => depth -= 1,
                        Event::Scalar(..) | Event::Alias(..) => {}
                        _ => return Err(InvalidWorkflow),
                    }
                }
                Ok(())
            }
            _ => Err(InvalidWorkflow),
        }
    }
    fn triggers(&mut self) -> Result<bool, InvalidWorkflow> {
        match self.peek()? {
            Event::Scalar(..) => Ok(self.string_scalar()? == "workflow_dispatch"),
            Event::SequenceStart(..) => {
                self.take()?;
                let mut names = BTreeSet::new();
                while !matches!(self.peek()?, Event::SequenceEnd) {
                    let name = self.string_scalar()?;
                    if !names.insert(name) {
                        return Err(InvalidWorkflow);
                    }
                }
                self.expect(Event::SequenceEnd)?;
                Ok(names.contains("workflow_dispatch"))
            }
            Event::MappingStart(..) => {
                self.take()?;
                let mut names = BTreeSet::new();
                while !matches!(self.peek()?, Event::MappingEnd) {
                    let name = self.string_scalar()?;
                    if !names.insert(name.clone()) {
                        return Err(InvalidWorkflow);
                    }
                    if name == "workflow_dispatch" {
                        match self.peek()? {
                            Event::MappingStart(..) => self.skip_node()?,
                            Event::Scalar(value, TScalarStyle::Plain, _, None)
                                if value.is_empty() || value == "null" || value == "~" =>
                            {
                                self.take()?;
                            }
                            _ => return Err(InvalidWorkflow),
                        }
                    } else {
                        self.skip_node()?;
                    }
                }
                self.expect(Event::MappingEnd)?;
                Ok(names.contains("workflow_dispatch"))
            }
            _ => Err(InvalidWorkflow),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recognizes_dispatch_in_scalar_sequence_and_mapping_forms() {
        for text in [
            "on: workflow_dispatch\n",
            "on: [push, workflow_dispatch]\n",
            "'on':\n  workflow_dispatch:\n",
            "on: {workflow_dispatch: {}, push: {branches: [main]}}\n",
            "on:\n  workflow_dispatch:\n    inputs:\n      name:\n        type: string\n",
            "shared: &steps [one, two]\non: workflow_dispatch\njobs: *steps\n",
        ] {
            assert_eq!(has_dispatch(text.as_bytes()), Ok(true), "{text}");
        }
        for text in [
            "on: push\n",
            "on: [push, pull_request]\n",
            "on: {push: {}}\n",
            "name: workflow_dispatch\n",
            "jobs:\n  workflow_dispatch: {}\n",
        ] {
            assert_eq!(has_dispatch(text.as_bytes()), Ok(false), "{text}");
        }
    }
    #[test]
    fn accepts_the_project_workflow_example() {
        let example = include_bytes!("../examples/github-actions/sluss-ci.yml");
        assert_eq!(has_dispatch(example), Ok(true));
    }
    #[test]
    fn rejects_ambiguous_or_unbounded_trigger_declarations() {
        for text in [
            "on: push\non: workflow_dispatch\n",
            "on: [workflow_dispatch, workflow_dispatch]\n",
            "on: {workflow_dispatch: {}, workflow_dispatch: {}}\n",
            "on: {workflow_dispatch: true}\n",
            "on: {workflow_dispatch: 'null'}\n",
            "events: &e [workflow_dispatch]\non: *e\n",
            "on: workflow_dispatch\n---\non: push\n",
            "on: [workflow_dispatch\n",
        ] {
            assert_eq!(
                has_dispatch(text.as_bytes()),
                Err(InvalidWorkflow),
                "{text}"
            );
        }
        assert_eq!(
            has_dispatch(&vec![b'a'; MAX_BYTES + 1]),
            Err(InvalidWorkflow)
        );
        let deep = format!(
            "on: workflow_dispatch\njobs: {}x{}\n",
            "[".repeat(65),
            "]".repeat(65)
        );
        assert_eq!(has_dispatch(deep.as_bytes()), Err(InvalidWorkflow));
        assert_eq!(has_dispatch(&[0xff]), Err(InvalidWorkflow));
        let excessive = format!(
            "on: workflow_dispatch\njobs: [{}]\n",
            "x,".repeat(MAX_EVENTS)
        );
        assert_eq!(has_dispatch(excessive.as_bytes()), Err(InvalidWorkflow));
    }
}
