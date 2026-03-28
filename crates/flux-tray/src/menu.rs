// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tray context menu construction and update helpers.

use tray_icon::menu::accelerator::Accelerator;
use tray_icon::menu::{CheckMenuItem, Menu, MenuId, MenuItem, PredefinedMenuItem, Submenu};

/// Holds references to menu items we need to interact with after creation.
pub struct TrayMenu {
    pub menu: Menu,
    pub open_item: MenuItem,
    pub recent_submenu: Submenu,
    pub notifications_item: CheckMenuItem,
    pub stop_item: MenuItem,
}

const NO_ACCEL: Option<Accelerator> = None;

/// Build the initial tray context menu.
pub fn build_menu(version: &str, notifications_enabled: bool) -> TrayMenu {
    let menu = Menu::new();

    let open_item = MenuItem::with_id(MenuId::new("open"), "Open Horizon Flux", true, NO_ACCEL);
    let recent_submenu = Submenu::with_id(MenuId::new("recent"), "Recent Runs", true);
    let no_runs = MenuItem::with_id(MenuId::new("no-runs"), "(no runs yet)", false, NO_ACCEL);
    let _ = recent_submenu.append(&no_runs);

    let notifications_item = CheckMenuItem::with_id(
        MenuId::new("notifications"),
        "Notifications",
        true,
        notifications_enabled,
        NO_ACCEL,
    );

    let stop_item = MenuItem::with_id(MenuId::new("stop"), "Stop Server", true, NO_ACCEL);
    let version_item = MenuItem::with_id(
        MenuId::new("version"),
        format!("Version: {version}"),
        false,
        NO_ACCEL,
    );

    let _ = menu.append(&open_item);
    let _ = menu.append(&recent_submenu);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&notifications_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&stop_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&version_item);

    TrayMenu {
        menu,
        open_item,
        recent_submenu,
        notifications_item,
        stop_item,
    }
}

/// Replace the contents of the "Recent Runs" submenu.
pub fn update_recent_runs(submenu: &Submenu, runs: &[crate::RecentRun]) {
    while submenu.remove_at(0).is_some() {}

    if runs.is_empty() {
        let placeholder =
            MenuItem::with_id(MenuId::new("no-runs"), "(no runs yet)", false, NO_ACCEL);
        let _ = submenu.append(&placeholder);
        return;
    }

    for run in runs {
        let item = MenuItem::with_id(run.menu_id.clone(), &run.label, true, NO_ACCEL);
        let _ = submenu.append(&item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires main thread (macOS constraint)"]
    fn build_menu_does_not_panic() {
        let tray_menu = build_menu("0.1.0", true);
        assert_eq!(tray_menu.open_item.id(), &MenuId::new("open"));
        assert_eq!(tray_menu.stop_item.id(), &MenuId::new("stop"));
    }
}
