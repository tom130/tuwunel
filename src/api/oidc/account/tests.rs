use http::{StatusCode, header::CONTENT_SECURITY_POLICY};
use tuwunel_core::utils::html::TUWUNEL_CSP_VALUE;

use super::{
	ACCOUNT_MANAGEMENT_ACTIONS_SUPPORTED, account_html_response_with_form_action,
	normalize_account_action,
};

#[test]
fn authorization_csp_only_widens_form_action() {
	let strict = account_html_response_with_form_action(StatusCode::OK, String::new(), None);
	let widened = account_html_response_with_form_action(
		StatusCode::OK,
		String::new(),
		Some("https://element.example"),
	);

	assert_eq!(
		strict.headers()[CONTENT_SECURITY_POLICY],
		TUWUNEL_CSP_VALUE
	);
	assert_eq!(
		widened.headers()[CONTENT_SECURITY_POLICY],
		"default-src 'none';script-src 'self';style-src 'self';frame-ancestors 'none';form-action 'self' https://element.example;base-uri 'none'"
	);
}

#[test]
fn stable_names_map_to_aliases() {
	assert_eq!(normalize_account_action("org.matrix.devices_list"), "org.matrix.sessions_list");
	assert_eq!(normalize_account_action("org.matrix.device_view"), "org.matrix.session_view");
	assert_eq!(normalize_account_action("org.matrix.device_delete"), "org.matrix.session_end");
}

#[test]
fn aliases_and_others_pass_through() {
	for action in [
		"org.matrix.sessions_list",
		"org.matrix.session_view",
		"org.matrix.session_end",
		"org.matrix.profile",
		"org.matrix.account_deactivate",
		"org.matrix.cross_signing_reset",
	] {
		assert_eq!(normalize_account_action(action), action);
	}
}

#[test]
fn stable_actions_are_advertised() {
	for action in [
		"org.matrix.profile",
		"org.matrix.devices_list",
		"org.matrix.device_view",
		"org.matrix.device_delete",
		"org.matrix.account_deactivate",
		"org.matrix.cross_signing_reset",
	] {
		assert!(ACCOUNT_MANAGEMENT_ACTIONS_SUPPORTED.contains(&action));
	}
}
