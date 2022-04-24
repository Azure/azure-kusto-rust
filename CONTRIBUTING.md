# Contributing to Azure Rust SDK

If you would like to become an active contributor to this project please
follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](https://azure.github.io/azure-sdk/general_introduction.html).

## Building and Testing

This project uses the [mock_transport_framework](https://github.com/Azure/azure-sdk-for-rust/blob/main/docs/mock_transport.md)
from the unofficial azure Rust SDKs. The main idea is to record interactions with external services (Kusto) locally, and replay
the responses in CI/CD and for normal testing. This is particularly useful for end to end tests.

To execute tests against recorded responses run:

```bash
cargo test --features=mock_transport_framework
```

> Using `cargo test` will also work, but omit all tests requiring the mock_transport_framework

To record new transactions, first place a `.env` file (omitted by `.gitignore`) in the repository root

```toml
AZURE_CLIENT_ID="..."
AZURE_CLIENT_SECRET="..."
AZURE_TENANT_ID="..."
KUSTO_SERVICE_URL="..."
KUSTO_DATABASE="..."
```

> The provided service principal needs to be able to `create` and `drop` tables in the specified database

Then execute tests in `RECORD` mode:

```bash
TESTING_MODE=RECORD cargo test --features=mock_transport_framework
```

> While all credentials and identifiable urls are stripped from recordings, the used database name, the query
> and responses are committed to source control. So make sure no sensitive data is contained therein. Care
> should also be taken to reduce the information about DB internals returned from a query - especially
> when using control commands.

## Style

We use `fmt` and `clippy` for formatting and linting:

```bash
cargo fmt
cargo clippy
```

## PRs

We welcome contributions. In order to make the PR process efficient, please follow the below checklist:

- **There is an issue open concerning the code added** - (either bug or enhancement).
  Preferably there is an agreed upon approach in the issue.
- **PR comment explains the changes done** - (This should be a TL;DR; as the rest of it should be documented in the related issue).
- **PR is concise** - try and avoid make drastic changes in a single PR. Split it into multiple changes if possible. If you feel a major change is needed, it is ok, but make sure commit history is clear and one of the maintainers can comfortably review both the code and the logic behind the change.
- **Please provide any related information needed to understand the change** - docs, guidelines, use-case, best practices and so on. Opinions are accepted, but have to be backed up.
- **Checks should pass** - these including linting with cargo fmt and clippy and running tests.

## Code of Conduct

This project's code of conduct can be found in the
[CODE_OF_CONDUCT.md file](https://github.com/Azure/azure-sdk-for-rust/blob/master/CODE_OF_CONDUCT.md)
(v1.4.0 of the http://contributor-covenant.org/ CoC).
