use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use tuwunel_core::utils::hash::sha256;

pub const S256: &str = "S256";

#[must_use]
pub fn verify_s256(code_verifier: &str, code_challenge: &str) -> bool {
	let expected = URL_SAFE_NO_PAD.encode(sha256::hash(code_verifier.as_bytes()));

	constant_time_eq(&expected, code_challenge)
}

fn constant_time_eq(left: &str, right: &str) -> bool {
	let left = left.as_bytes();
	let right = right.as_bytes();
	let max_len = left.len().max(right.len());
	let mut diff = left.len() ^ right.len();

	for index in 0..max_len {
		let left_byte = left.get(index).copied().unwrap_or(0);
		let right_byte = right.get(index).copied().unwrap_or(0);
		diff |= usize::from(left_byte ^ right_byte);
	}

	diff == 0
}

#[cfg(test)]
mod tests {
	use super::verify_s256;

	#[test]
	fn pkce_s256_verifies_rfc7636_vector() {
		assert!(verify_s256(
			"dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
			"E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
		));
	}

	#[test]
	fn pkce_s256_rejects_wrong_or_non_canonical_challenges() {
		let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";

		assert!(!verify_s256(verifier, "wrong"));
		assert!(!verify_s256(verifier, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM=",));
		assert!(!verify_s256(verifier, "e9melhoa2owvfremtjguchaoek1t8urwbugjsstw-cm",));
	}
}
