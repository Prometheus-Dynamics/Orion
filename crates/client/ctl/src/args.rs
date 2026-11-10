//! The command line, taken apart: words in order, and options by name.

/// What is left of a command line after the words before it were taken.
#[derive(Debug)]
pub(crate) struct Args {
    words: Vec<String>,
}

/// A command line that cannot be understood, and why.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Usage(pub(crate) String);

impl Args {
    pub(crate) fn new(words: &[String]) -> Self {
        Self {
            words: words.to_vec(),
        }
    }

    /// Takes the option `--name value` out, wherever it is.
    pub(crate) fn option(&mut self, name: &str) -> Result<Option<String>, Usage> {
        let Some(at) = self.words.iter().position(|word| word == name) else {
            return Ok(None);
        };
        if at + 1 >= self.words.len() {
            return Err(Usage(format!("{name} needs a value")));
        }
        self.words.remove(at);
        Ok(Some(self.words.remove(at)))
    }

    /// Takes the option `--name value` out as a number.
    pub(crate) fn number(&mut self, name: &str) -> Result<Option<u64>, Usage> {
        match self.option(name)? {
            None => Ok(None),
            Some(text) => text
                .parse()
                .map(Some)
                .map_err(|_| Usage(format!("{name} needs a number, not {text:?}"))),
        }
    }

    /// Takes the flag `--name` out. Whether it was there.
    pub(crate) fn flag(&mut self, name: &str) -> bool {
        match self.words.iter().position(|word| word == name) {
            Some(at) => {
                self.words.remove(at);
                true
            }
            None => false,
        }
    }

    /// Takes the next word that is not an option.
    pub(crate) fn word(&mut self, what: &str) -> Result<String, Usage> {
        match self.words.first() {
            Some(word) if !word.starts_with("--") => Ok(self.words.remove(0)),
            _ => Err(Usage(format!("missing {what}"))),
        }
    }

    /// Takes the next word that is not an option, if there is one.
    pub(crate) fn maybe_word(&mut self) -> Option<String> {
        match self.words.first() {
            Some(word) if !word.starts_with("--") => Some(self.words.remove(0)),
            _ => None,
        }
    }

    /// Takes every word that is left.
    pub(crate) fn rest(&mut self) -> Vec<String> {
        std::mem::take(&mut self.words)
    }

    /// Fails if anything was not understood.
    pub(crate) fn done(&self) -> Result<(), Usage> {
        match self.words.first() {
            None => Ok(()),
            Some(word) => Err(Usage(format!("{word:?} is not understood here"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Args {
        Args::new(
            &line
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn options_are_taken_from_anywhere_and_words_in_order() {
        let mut args = args("fan.left --for 500 set --detached --args fast");
        assert_eq!(args.number("--for"), Ok(Some(500)));
        assert!(args.flag("--detached") && !args.flag("--detached"));
        assert_eq!(args.option("--args"), Ok(Some("fast".into())));
        assert_eq!(args.option("--missing"), Ok(None));
        assert_eq!(args.word("a name"), Ok("fan.left".into()));
        assert_eq!(args.maybe_word(), Some("set".into()));
        assert_eq!((args.maybe_word(), args.done()), (None, Ok(())));
    }

    #[test]
    fn what_is_not_understood_is_said() {
        let mut args = args("--for soon extra --kind");
        assert_eq!(
            args.number("--for"),
            Err(Usage("--for needs a number, not \"soon\"".into()))
        );
        assert_eq!(
            args.option("--kind"),
            Err(Usage("--kind needs a value".into()))
        );
        assert_eq!(
            args.done(),
            Err(Usage("\"extra\" is not understood here".into()))
        );
        assert_eq!(
            Args::new(&[]).word("a topic"),
            Err(Usage("missing a topic".into()))
        );
        assert_eq!(
            Args::new(&["--flag".into()]).word("a topic"),
            Err(Usage("missing a topic".into()))
        );
    }
}
