# Typeline Documentation

## Useful Snippets

### Rename files 1,2,3 to 001,002,003:
```bash
# create dummy files
tl seq=10 sh="touch {}.txt"

# rename
ls -1 | tl lines r="\d+" [ exec mv {}.txt {:02}.txt ]
```
