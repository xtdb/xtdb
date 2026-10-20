# Contributing to XTDB

Welcome - and first of all, thanks!

XTDB would not exist without the community of vibrant open source projects on which it depends, and we hope that the XTDB community will serve to extend and reflect our gratitude.

## How to contribute

We use [GitHub Issues](https://github.com/xtdb/xtdb/issues) to work on near-term changes to the XTDB codebase and documentation.
Our work-in-progress is shown on our [GitHub Project Board](https://github.com/orgs/xtdb/projects/13).

PRs with fixes and improvements to our documentation are also very welcome.

A Contributor License Agreement (CLA) is necessary for us to ensure that we can support a healthy XTDB ecosystem over the indefinite future.
The agreement is [dev/cla-individual.md](dev/cla-individual.md), and you sign it with a commit in your PR:

- add the trailer `CLA-Agreed: /dev/cla-individual.md, as of this commit` to the commit message, and
- sign the commit with a key that GitHub shows as [verified](https://docs.github.com/en/authentication/managing-commit-signature-verification) on your account.

The commit's author stands as the signatory, so it has to be a commit you both author and sign yourself - each author in a PR signs with their own commit.
If your branch predates `dev/cla-individual.md`, rebase it onto `main` first, so that the commit contains the agreement it refers to.

One-time setup, in your clone:

```sh
git config trailer.cla.key CLA-Agreed
git config trailer.cla.cmd 'echo "/dev/cla-individual.md, as of this commit"'
```

Then `--trailer cla` adds the trailer to a commit:

```sh
git commit -S --trailer cla
```

The agreement covers your present and future contributions, so it's required on your first PR - but we'd appreciate the trailer on later ones too.
We merge your commits as they are, without rebasing or squashing them, so that the signed commit stays in the history.

If you can't sign commits, you can instead complete the very short [.pdf template](https://raw.githubusercontent.com/xtdb/xtdb/main/docs/xtdb-individual-contributor-license-agreement.pdf) ([.rtf source](https://raw.githubusercontent.com/xtdb/xtdb/main/docs/xtdb-individual-contributor-license-agreement.rtf)) and email it to us, hello@xtdb.com, along with a reference to your current PR on GitHub.

Particularly, **please do not open an issue to ask a question** - you'll get quicker support using our support channels below. Thanks!

## Releases

Our releases are published using [GitHub releases](https://github.com/xtdb/xtdb/releases) - feel free to 'watch' the XTDB repo to receive notifications.

## Get in touch!

Beyond GitHub, community support can be found on [Discord](https://github.com/orgs/xtdb/discussions/4385), where we'll be more than happy to answer questions and guide you through contributing to XTDB.
We also frequent the #xtdb channel on [Clojurians Slack](http://clojurians.net/)

Cheers!

XTDB Team
