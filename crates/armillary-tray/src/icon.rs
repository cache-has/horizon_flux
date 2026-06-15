// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Programmatically generated tray icons at 32×32.
//!
//! These are simple solid-color icons with a recognizable "H" glyph.
//! A proper designer icon can replace these later.

use tray_icon::Icon;

const SIZE: u32 = 32;

/// Idle state: blue icon.
pub fn idle_icon() -> Icon {
    generate_icon([0x33, 0x7A, 0xB7, 0xFF])
}

/// Running state: green icon.
pub fn running_icon() -> Icon {
    generate_icon([0x2E, 0xCC, 0x71, 0xFF])
}

/// Error state: red icon.
pub fn error_icon() -> Icon {
    generate_icon([0xE7, 0x4C, 0x3C, 0xFF])
}

/// Generate a 32×32 icon filled with `color` and a white "H" glyph.
fn generate_icon(color: [u8; 4]) -> Icon {
    let total = (SIZE * SIZE) as usize;
    let mut rgba = vec![0u8; total * 4];

    for i in 0..total {
        let x = (i % SIZE as usize) as u32;
        let y = (i / SIZE as usize) as u32;
        let offset = i * 4;

        if is_h_pixel(x, y, SIZE) {
            // White "H"
            rgba[offset] = 0xFF;
            rgba[offset + 1] = 0xFF;
            rgba[offset + 2] = 0xFF;
            rgba[offset + 3] = 0xFF;
        } else {
            rgba[offset] = color[0];
            rgba[offset + 1] = color[1];
            rgba[offset + 2] = color[2];
            rgba[offset + 3] = color[3];
        }
    }

    Icon::from_rgba(rgba, SIZE, SIZE).expect("icon dimensions are correct")
}

/// Returns true if (x, y) falls within the "H" letter shape.
/// Coordinates are in a `size × size` grid.
fn is_h_pixel(x: u32, y: u32, size: u32) -> bool {
    let margin = size / 5; // 6px on 32
    let stroke = size / 8; // 4px on 32

    let left_col = x >= margin && x < margin + stroke;
    let right_col = x >= size - margin - stroke && x < size - margin;
    let crossbar = y >= (size / 2 - stroke / 2)
        && y < (size / 2 + stroke / 2)
        && x >= margin
        && x < size - margin;
    let in_y_range = y >= margin && y < size - margin;

    ((left_col || right_col) && in_y_range) || crossbar
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icons_are_valid() {
        // Just ensure they don't panic.
        let _ = idle_icon();
        let _ = running_icon();
        let _ = error_icon();
    }

    #[test]
    fn h_pixel_hits_expected_regions() {
        // Left column top
        assert!(is_h_pixel(6, 8, 32));
        // Right column top
        assert!(is_h_pixel(24, 8, 32));
        // Center crossbar
        assert!(is_h_pixel(16, 16, 32));
        // Outside margins
        assert!(!is_h_pixel(0, 0, 32));
    }
}
