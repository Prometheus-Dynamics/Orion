//! What `orion/boards` says, shared by the responder that hears the other boards (`announce`)
//! and the file that says it (`own_files`): either can be built without the other.

use std::cell::RefCell;
use std::rc::Rc;

/// What `orion/boards` says when it is read, once the responder that hears them runs.
#[derive(Clone, Default)]
pub(crate) struct Heard(Rc<RefCell<Option<Text>>>);

/// What a file says when it is read.
type Text = Box<dyn Fn() -> String>;

impl Heard {
    /// From now, `orion/boards` says what `text` gives.
    #[cfg(feature = "announce")]
    pub(crate) fn from(&self, text: impl Fn() -> String + 'static) {
        *self.0.borrow_mut() = Some(Box::new(text));
    }

    /// What it says now, if the responder runs.
    #[cfg(feature = "offers")]
    pub(crate) fn said(&self) -> Option<String> {
        self.0.borrow().as_ref().map(|text| text())
    }
}

impl std::fmt::Debug for Heard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Heard")
    }
}
