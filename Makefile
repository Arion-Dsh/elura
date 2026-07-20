.PHONY: fmt fmt-fix check test docs package verify

fmt:
	cargo fmt --all -- --check

fmt-fix:
	cargo fmt --all

check:
	cargo clippy --workspace --all-features --all-targets -- -D warnings

test:
	cargo test --workspace --all-features

docs:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --lib

package:
	cargo package -p elura-core --allow-dirty --no-verify
	cargo package -p elura-runtime --allow-dirty --list
	cargo package -p elura-gateway --allow-dirty --list
	cargo package -p elura-world --allow-dirty --list
	cargo package -p elura-room --allow-dirty --list
	cargo package -p elura-aoi --allow-dirty --list
	cargo package -p elura-simulation --allow-dirty --list
	cargo package -p elura-netcode --allow-dirty --list
	cargo package -p elura-replication --allow-dirty --list
	cargo package -p elura-lag-compensation --allow-dirty --list
	cargo package -p elura-net-sim --allow-dirty --list
	cargo package -p elura-monolith --allow-dirty --list
	cargo package -p elura-adapters --allow-dirty --list
	cargo package -p elura-providers --allow-dirty --list
	cargo package -p elura --allow-dirty --list
	cargo package -p elura-cli --allow-dirty --list

verify: fmt check test docs package
