//! The board's accounts, read from its own files. No library is asked: the comm core is one
//! static file, and these two files are the whole of what a login needs to know.

use std::path::{Path, PathBuf};

/// One account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
    /// Its name.
    pub name: String,
    /// Its user id.
    pub uid: u32,
    /// Its own group.
    pub gid: u32,
    /// Every group it is in, its own first.
    pub groups: Vec<u32>,
    /// Its home directory.
    pub home: PathBuf,
    /// Its shell.
    pub shell: PathBuf,
}

/// The account named `name` in the text of a `passwd` file, with its groups from the text
/// of a `group` file. `None` if there is none, or its line is not one.
#[must_use]
pub fn find(passwd: &str, group: &str, name: &str) -> Option<User> {
    let line = passwd
        .lines()
        .find(|line| line.split(':').next() == Some(name))?;
    let mut parts = line.split(':');
    let (name, _password, uid, gid, _comment, home, shell) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next().unwrap_or(""),
    );
    let (uid, gid) = (uid.parse().ok()?, gid.parse().ok()?);
    let mut groups = vec![gid];
    for line in group.lines() {
        // name:password:gid:member,member
        let mut parts = line.split(':');
        let (Some(_), Some(_), Some(id), Some(members)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if let Ok(id) = id.parse::<u32>()
            && !groups.contains(&id)
            && members.split(',').any(|member| member.trim() == name)
        {
            groups.push(id);
        }
    }
    Some(User {
        name: name.to_owned(),
        uid,
        gid,
        groups,
        home: if home.is_empty() {
            "/".into()
        } else {
            home.into()
        },
        shell: if shell.is_empty() {
            "/bin/sh".into()
        } else {
            shell.into()
        },
    })
}

/// The account named `name` on this board, from the `passwd` and `group` files in `etc`.
#[must_use]
pub fn lookup(etc: &Path, name: &str) -> Option<User> {
    let passwd = std::fs::read_to_string(etc.join("passwd")).ok()?;
    let group = std::fs::read_to_string(etc.join("group")).unwrap_or_default();
    find(&passwd, &group, name)
}

/// The account with user id `uid` on this board.
#[must_use]
pub fn lookup_uid(etc: &Path, uid: u32) -> Option<User> {
    let passwd = std::fs::read_to_string(etc.join("passwd")).ok()?;
    let name = passwd.lines().find_map(|line| {
        let mut parts = line.split(':');
        let name = parts.next()?;
        (parts.nth(1)?.parse::<u32>().ok()? == uid).then_some(name)
    })?;
    let group = std::fs::read_to_string(etc.join("group")).unwrap_or_default();
    find(&passwd, &group, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASSWD: &str = "root:x:0:0:root:/root:/bin/bash\n\
                          # a comment\n\
                          ana:x:1000:1000:Ana,,,:/home/ana:/bin/zsh\n\
                          svc:x:990:985::/var/lib/svc:\n\
                          broken:x:abc:1::/:/bin/sh\n\
                          short:x:5\n";
    const GROUP: &str =
        "root:x:0:\nana:x:1000:\nvideo:x:44:ana,svc\ndialout:x:20:other, ana\nbad:x:x:ana\n";

    #[test]
    fn an_account_is_its_line_and_the_groups_that_name_it() {
        let ana = find(PASSWD, GROUP, "ana").expect("ana");
        assert_eq!((ana.uid, ana.gid), (1000, 1000));
        assert_eq!(ana.groups, [1000, 44, 20]);
        assert_eq!(
            (ana.home.to_str(), ana.shell.to_str()),
            (Some("/home/ana"), Some("/bin/zsh"))
        );
        let root = find(PASSWD, GROUP, "root").expect("root");
        assert_eq!((root.uid, root.groups.as_slice()), (0, [0].as_slice()));
        // No shell named is the plain one.
        assert_eq!(
            find(PASSWD, GROUP, "svc").expect("svc").shell.to_str(),
            Some("/bin/sh")
        );
    }

    #[test]
    fn nobody_is_made_up() {
        for name in [
            "",
            "an",
            "anab",
            "nobody",
            "broken",
            "short",
            "# a comment",
            "x",
        ] {
            assert_eq!(find(PASSWD, GROUP, name), None, "{name}");
        }
    }
}
