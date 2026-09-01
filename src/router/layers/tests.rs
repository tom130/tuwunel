#![cfg(test)]

use axum::Extension;
use http::header;
use ipnet::IpNet;
use tower::{Layer, ServiceExt, util::Either};
use tuwunel_api::router::{ConfiguredIpSource, TrustedPeerSubnets};
use tuwunel_core::{config::IpSource, utils::html::TUWUNEL_CSP_VALUE};

use super::{ip_source_layer, trusted_peer_subnets_layer};

#[test]
fn ip_source_layer_none_returns_identity_branch() {
	let layer = ip_source_layer(None);

	assert!(matches!(layer, Either::Right(_)));
}

#[test]
fn ip_source_layer_connect_info_returns_extension_branch() {
	let layer = ip_source_layer(Some(IpSource::ConnectInfo));

	assert!(matches!(layer, Either::Left(Extension(ConfiguredIpSource(_)))));
}

#[test]
fn trusted_peer_subnets_layer_empty_returns_identity_branch() {
	let layer = trusted_peer_subnets_layer(&[]);

	assert!(matches!(layer, Either::Right(_)));
}

#[test]
fn trusted_peer_subnets_layer_populated_returns_extension_branch() {
	let subnets: Vec<IpNet> =
		vec!["172.18.0.0/16".parse().expect("CIDR"), "fd00::/8".parse().expect("CIDR")];

	let layer = trusted_peer_subnets_layer(&subnets);

	let nets = match layer {
		| Either::Left(Extension(TrustedPeerSubnets(nets))) => nets,
		| Either::Right(_) => panic!("expected extension branch"),
	};

	assert_eq!(nets.len(), 2);
}

#[tokio::test]
async fn html_layer_preserves_handler_csp() {
	let service =
		super::html_layer().layer(tower::service_fn(async |_request: http::Request<()>| {
			Ok::<_, std::convert::Infallible>(
				http::Response::builder()
					.header(header::CONTENT_TYPE, "text/html")
					.header(
						header::CONTENT_SECURITY_POLICY,
						"form-action 'self' https://client.example",
					)
					.body(())
					.expect("static test response must build"),
			)
		}));

	let response = service
		.oneshot(http::Request::new(()))
		.await
		.expect("infallible service");

	assert_eq!(
		response.headers()[header::CONTENT_SECURITY_POLICY],
		"form-action 'self' https://client.example"
	);
	assert_eq!(response.headers()[header::X_FRAME_OPTIONS], "DENY");
}

#[tokio::test]
async fn html_layer_applies_default_csp() {
	let service =
		super::html_layer().layer(tower::service_fn(async |_request: http::Request<()>| {
			Ok::<_, std::convert::Infallible>(
				http::Response::builder()
					.header(header::CONTENT_TYPE, "text/html")
					.body(())
					.expect("static test response must build"),
			)
		}));

	let response = service
		.oneshot(http::Request::new(()))
		.await
		.expect("infallible service");

	assert_eq!(response.headers()[header::CONTENT_SECURITY_POLICY], TUWUNEL_CSP_VALUE);
	assert_eq!(response.headers()[header::X_FRAME_OPTIONS], "DENY");
}

#[tokio::test]
async fn html_layer_leaves_non_html_response_untouched() {
	let service =
		super::html_layer().layer(tower::service_fn(async |_request: http::Request<()>| {
			Ok::<_, std::convert::Infallible>(
				http::Response::builder()
					.header(header::CONTENT_TYPE, "application/json")
					.body(())
					.expect("static test response must build"),
			)
		}));

	let response = service
		.oneshot(http::Request::new(()))
		.await
		.expect("infallible service");

	assert!(
		!response
			.headers()
			.contains_key(header::CONTENT_SECURITY_POLICY)
	);
	assert!(
		!response
			.headers()
			.contains_key(header::X_FRAME_OPTIONS)
	);
}
