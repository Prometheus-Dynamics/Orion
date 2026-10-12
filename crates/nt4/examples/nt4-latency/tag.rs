//! The AprilTag the camera looks for, drawn as SVG from the tag36h11 bit pattern. No network
//! access: the code table is in `tag36h11.rs`.

use std::fmt::Write as _;

use crate::tag36h11::{CODES, COUNT};

/// The display page with the tag for `id` drawn in.
pub fn page_html(id: u32) -> Result<String, String> {
    let svg = tag_svg(id)?;
    Ok(include_str!("page.html")
        .replace("{{TAG_SVG}}", &svg)
        .replace("{{TAG_ID}}", &id.to_string()))
}

/// The tag as SVG: an 8x8 cell square (a black border around 6x6 data cells, where a set bit is a
/// white cell), with one white cell of quiet zone around it.
pub fn tag_svg(id: u32) -> Result<String, String> {
    let code = CODES
        .get(id as usize)
        .copied()
        .ok_or_else(|| format!("tag id {id} is not in tag36h11 (0..{COUNT})"))?;
    let mut svg = String::from(
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="-1 -1 10 10" shape-rendering="crispEdges">"#,
    );
    svg.push_str(r##"<rect x="-1" y="-1" width="10" height="10" fill="#fff"/>"##);
    svg.push_str(r##"<rect width="8" height="8" fill="#000"/>"##);
    for row in 0..6 {
        for col in 0..6 {
            let bit = (code >> (35 - (row * 6 + col))) & 1;
            if bit == 1 {
                let _ = write!(
                    svg,
                    r##"<rect x="{}" y="{}" width="1" height="1" fill="#fff"/>"##,
                    col + 1,
                    row + 1
                );
            }
        }
    }
    svg.push_str("</svg>");
    Ok(svg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_zero_top_row_matches_the_family_table() {
        // Id 0 is 0x21a146bab: the top row (bits 35..30) is 0,0,1,0,0,0.
        let svg = tag_svg(0).unwrap();
        let data_cells = svg.matches(r#"width="1""#).count();
        assert_eq!(data_cells, (0x21a146bab_u64).count_ones() as usize);
        // The first white cell is column 2 of row 0.
        assert!(svg.contains(r##"<rect x="3" y="1" width="1" height="1" fill="#fff"/>"##));
        assert!(!svg.contains(r##"<rect x="1" y="1" width="1" height="1" fill="#fff"/>"##));
    }

    #[test]
    fn ids_outside_the_family_are_rejected() {
        assert!(tag_svg(586).is_ok());
        assert!(tag_svg(587).is_err());
        assert!(page_html(1).unwrap().contains("<svg"));
    }
}
