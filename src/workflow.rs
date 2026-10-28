//! Bounded, conservative recognition of Sluss-compatible dispatch inputs.
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

/// Return whether the document explicitly declares the trigger and the two
/// string inputs sent by Sluss, without any other mandatory inputs.
/// Aliases outside `on` are ignored without expansion; an aliased trigger is
/// rejected because its value cannot be proven by this streaming inspection.
pub(crate) fn supports_sluss_dispatch(bytes: &[u8]) -> Result<bool, InvalidWorkflow> {
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
        if key == "<<" || !keys.insert(key.clone()) {
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
            Event::Scalar(..) => {
                self.string_scalar()?;
                Ok(false)
            }
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
                Ok(false)
            }
            Event::MappingStart(..) => {
                self.take()?;
                let mut names = BTreeSet::new();
                let mut compatible = false;
                while !matches!(self.peek()?, Event::MappingEnd) {
                    let name = self.string_scalar()?;
                    if name == "<<" || !names.insert(name.clone()) {
                        return Err(InvalidWorkflow);
                    }
                    if name == "workflow_dispatch" {
                        match self.peek()? {
                            Event::MappingStart(..) => compatible = self.dispatch_config()?,
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
                Ok(compatible)
            }
            _ => Err(InvalidWorkflow),
        }
    }

    fn dispatch_config(&mut self) -> Result<bool, InvalidWorkflow> {
        if !matches!(self.take()?, Event::MappingStart(..)) {
            return Err(InvalidWorkflow);
        }
        let mut keys = BTreeSet::new();
        let mut compatible = false;
        while !matches!(self.peek()?, Event::MappingEnd) {
            let key = self.string_scalar()?;
            if key == "<<" || !keys.insert(key.clone()) {
                return Err(InvalidWorkflow);
            }
            if key == "inputs" {
                compatible = self.dispatch_inputs()?;
            } else {
                self.skip_node()?;
            }
        }
        self.expect(Event::MappingEnd)?;
        Ok(compatible)
    }

    fn dispatch_inputs(&mut self) -> Result<bool, InvalidWorkflow> {
        if !matches!(self.take()?, Event::MappingStart(..)) {
            return Err(InvalidWorkflow);
        }
        let mut names = BTreeSet::new();
        let mut host = false;
        let mut suites = false;
        let mut other_mandatory = false;
        while !matches!(self.peek()?, Event::MappingEnd) {
            let name = self.string_scalar()?;
            if name == "<<" || !names.insert(name.clone()) || names.len() > 25 {
                return Err(InvalidWorkflow);
            }
            let (kind, required) = self.input_definition()?;
            match name.as_str() {
                "sluss_host" => host = kind.as_deref() == Some("string"),
                "sluss_suites" => suites = kind.as_deref() == Some("string"),
                _ => other_mandatory |= required,
            }
        }
        self.expect(Event::MappingEnd)?;
        Ok(host && suites && !other_mandatory)
    }

    fn input_definition(&mut self) -> Result<(Option<String>, bool), InvalidWorkflow> {
        if !matches!(self.take()?, Event::MappingStart(..)) {
            return Err(InvalidWorkflow);
        }
        let mut keys = BTreeSet::new();
        let mut kind = None;
        let mut required = false;
        while !matches!(self.peek()?, Event::MappingEnd) {
            let key = self.string_scalar()?;
            if key == "<<" || !keys.insert(key.clone()) {
                return Err(InvalidWorkflow);
            }
            match key.as_str() {
                "type" => kind = Some(self.string_scalar()?),
                "required" => match self.take()? {
                    Event::Scalar(value, TScalarStyle::Plain, _, None) if value == "true" => {
                        required = true;
                    }
                    Event::Scalar(value, TScalarStyle::Plain, _, None) if value == "false" => {}
                    _ => return Err(InvalidWorkflow),
                },
                _ => self.skip_node()?,
            }
        }
        self.expect(Event::MappingEnd)?;
        Ok((kind, required))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requires_both_string_inputs_without_other_mandatory_inputs() {
        let good = "on: {workflow_dispatch: {inputs: {sluss_host: {type: string}, sluss_suites: {type: string}, note: {type: string, required: false}}}}\n";
        assert_eq!(supports_sluss_dispatch(good.as_bytes()), Ok(true));
        for text in [
            "on: workflow_dispatch\n".to_string(),
            "on: [push, workflow_dispatch]\n".to_string(),
            "on: {workflow_dispatch: {}}\n".to_string(),
            "on: push\n".to_string(),
            "on: {workflow_dispatch: {inputs: {sluss_host: {type: string}}}}\n".to_string(),
            "on: {workflow_dispatch: {inputs: {sluss_host: {type: string}, sluss_suites: {type: string}, extra: {type: string, required: true}}}}\n".to_string(),
            "on: {workflow_dispatch: {inputs: {sluss_host: {type: choice}, sluss_suites: {type: string}}}}\n".to_string(),
            "on: {workflow_dispatch: {inputs: {sluss_host: {}, sluss_suites: {type: string}}}}\n".to_string(),
        ] {
            assert_eq!(supports_sluss_dispatch(text.as_bytes()), Ok(false), "{text}");
        }
    }
    #[test]
    fn accepts_the_project_workflow_example() {
        let example = include_bytes!("../examples/github-actions/sluss-ci.yml");
        assert_eq!(supports_sluss_dispatch(example), Ok(true));
    }
    #[test]
    fn rejects_ambiguous_or_unbounded_trigger_declarations() {
        for text in [
            "on: push\non: workflow_dispatch\n",
            "on: [workflow_dispatch, workflow_dispatch]\n",
            "on: {workflow_dispatch: {}, workflow_dispatch: {}}\n",
            "on: {workflow_dispatch: true}\n",
            "on: {workflow_dispatch: 'null'}\n",
            "on: {workflow_dispatch: {inputs: {sluss_host: {type: string}, sluss_host: {type: string}}}}}\n",
            "on: {workflow_dispatch: {inputs: {sluss_host: {type: string, type: string}}}}}\n",
            "on: {workflow_dispatch: {inputs: {sluss_host: {type: string, required: 'false'}}}}}\n",
            "on: {workflow_dispatch: {inputs: {sluss_host: *alias}}}\n",
            "defaults: &d {required: true}\non: {workflow_dispatch: {inputs: {extra: {<<: *d}}}}}\n",
            "events: &e [workflow_dispatch]\non: *e\n",
            "on: workflow_dispatch\n---\non: push\n",
            "on: [workflow_dispatch\n",
        ] {
            assert_eq!(
                supports_sluss_dispatch(text.as_bytes()),
                Err(InvalidWorkflow),
                "{text}"
            );
        }
        assert_eq!(
            supports_sluss_dispatch(&vec![b'a'; MAX_BYTES + 1]),
            Err(InvalidWorkflow)
        );
        let deep = format!(
            "on: workflow_dispatch\njobs: {}x{}\n",
            "[".repeat(65),
            "]".repeat(65)
        );
        assert_eq!(
            supports_sluss_dispatch(deep.as_bytes()),
            Err(InvalidWorkflow)
        );
        assert_eq!(supports_sluss_dispatch(&[0xff]), Err(InvalidWorkflow));
        let inputs = (0..26)
            .map(|i| format!("i{i}: {{type: string}}"))
            .collect::<Vec<_>>()
            .join(", ");
        let too_many = format!("on: {{workflow_dispatch: {{inputs: {{{inputs}}}}}}}\n");
        assert_eq!(
            supports_sluss_dispatch(too_many.as_bytes()),
            Err(InvalidWorkflow)
        );
        let excessive = format!(
            "on: workflow_dispatch\njobs: [{}]\n",
            "x,".repeat(MAX_EVENTS)
        );
        assert_eq!(
            supports_sluss_dispatch(excessive.as_bytes()),
            Err(InvalidWorkflow)
        );
    }
}
