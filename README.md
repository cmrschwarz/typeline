# `TYPELINE`

[![github]](https://github.com/cmrschwarz/typeline)&ensp;
[![github-build]](https://github.com/cmrschwarz/typeline/actions/workflows/ci.yml)&ensp;

[github]: https://img.shields.io/badge/cmrschwarz/typeline-8da0cb?&labelColor=555555&logo=github
[github-build]: https://github.com/cmrschwarz/typeline/actions/workflows/ci.yml/badge.svg
[github-build-shields]: https://img.shields.io/github/actions/workflow/status/cmrschwarz/typeline/ci.yml?branch=main&logo=github

An efficient, type-safe pipeline processing language.


## Usage Examles

### Add Leading Zeroes to Numbered Files

```bash
ls | tl lines r="foo_(?<id>\d+)\.txt" mv="foo_{id:02}.txt"
```

### Advent of Code (Day 1, Part 1, 2023)
```bash
tl <input.txt lines fe: r-m='\d' fc: head next tail end join to_int end sum
```

### Download all PNG Images from a Website
```bash
tl str="https://google.com" GET xpath="//@href" r-f="\.png$" GET enum-n write="{:02}.png"
```

## License
[MIT](./LICENSE)
