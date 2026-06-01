#[must_use]
pub fn redirect_uri_matches(registered: &str, requested: &str) -> bool {
	if registered == requested {
		return true;
	}

	let Ok(registered) = url::Url::parse(registered) else {
		return false;
	};
	let Ok(requested) = url::Url::parse(requested) else {
		return false;
	};

	is_loopback_http(&registered)
		&& is_loopback_http(&requested)
		&& registered.scheme() == requested.scheme()
		&& registered.host_str() == requested.host_str()
		&& registered.path() == requested.path()
}

fn is_loopback_http(url: &url::Url) -> bool {
	url.scheme() == "http"
		&& matches!(url.host_str(), Some("127.0.0.1" | "::1" | "[::1]" | "localhost"))
}

#[cfg(test)]
mod tests {
	use super::redirect_uri_matches;

	#[test]
	fn redirect_uri_matches_exact_uris_and_custom_schemes() {
		assert!(redirect_uri_matches(
			"https://app.example/callback?fixed=1",
			"https://app.example/callback?fixed=1",
		));
		assert!(redirect_uri_matches(
			"com.example.app:/oauth2redirect",
			"com.example.app:/oauth2redirect",
		));

		assert!(!redirect_uri_matches(
			"https://app.example/callback",
			"https://app.example/other",
		));
		assert!(!redirect_uri_matches(
			"https://app.example/callback",
			"http://app.example/callback",
		));
		assert!(!redirect_uri_matches(
			"https://app.example/callback",
			"https://evil.example/callback",
		));
	}

	#[test]
	fn redirect_uri_allows_loopback_port_to_vary() {
		assert!(redirect_uri_matches(
			"http://127.0.0.1:1234/callback",
			"http://127.0.0.1:5678/callback",
		));
		assert!(
			redirect_uri_matches("http://[::1]:1234/callback", "http://[::1]:5678/callback",)
		);
		assert!(redirect_uri_matches(
			"http://localhost:1234/callback",
			"http://localhost:5678/callback",
		));
	}

	#[test]
	fn redirect_uri_loopback_still_requires_scheme_host_and_path() {
		assert!(!redirect_uri_matches(
			"http://127.0.0.1:1234/callback",
			"http://127.0.0.1:5678/other",
		));
		assert!(!redirect_uri_matches(
			"http://127.0.0.1:1234/callback",
			"https://127.0.0.1:5678/callback",
		));
		assert!(!redirect_uri_matches(
			"http://127.0.0.1:1234/callback",
			"http://localhost:5678/callback",
		));
		assert!(!redirect_uri_matches(
			"https://app.example:1234/callback",
			"https://app.example:5678/callback",
		));
	}
}
