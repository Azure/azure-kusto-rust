# Contributing to Azure Rust SDK

If you would like to become an active contributor to this project please
follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](https://azure.github.io/azure-sdk/general_introduction.html).

## Building and Testing

To fully test the end-to-end functionality, you will need to provide the following environment variables for your service principal and cluster: 

```toml
AZURE_CLIENT_ID="..."
AZURE_CLIENT_SECRET="..."
AZURE_TENANT_ID="..."
KUSTO_CLUSTER_URL="..."
KUSTO_DATABASE="..."
```

> The provided service principal needs to be able to `create` and `drop` tables in the specified database

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
[CODE_OF_CONDUCT.md file](https://github.com/Azure/azure-kusto-rust/blob/main/CODE_OF_CONDUCT.md)
(v1.4.0 of the http://contributor-covenant.org/ CoC).
