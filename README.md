# `BPL`

[![github]](https://github.com/cmrschwarz/scrr)&ensp;
[![github-build]](https://github.com/cmrschwarz/scrr/actions/workflows/ci.yml)&ensp;

[github]: https://img.shields.io/badge/cmrschwarz/scrr-8da0cb?&labelColor=555555&logo=github
[github-build]: https://github.com/cmrschwarz/scrr/actions/workflows/ci.yml/badge.svg
[github-build-shields]: https://img.shields.io/github/actions/workflow/status/cmrschwarz/scrr/ci.yml?branch=main&logo=github

A high performance batch processing language,
and a love letter to the CLI as a user interface.


## Usage Examles

### Add Leading Zeroes to Numbered Files

```bash
ls | bpl lines r="foo_(?<id>\d+)\.txt" mv="foo_{id:02}.txt"
```

### Advent of Code (Day 1, Part 1, 2023)
```bash
bpl <input.txt lines fe: r-m='\d' fc: head next tail end join to_int end sum
```

### Download all PNG Images from a Website
```bash
bpl str="https://google.com" GET xpath="//@href" r-f="\.png$" GET enum-n write="{:02}.png"
```

## License
[MIT](./LICENSE)
