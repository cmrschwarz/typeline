# SCR Documentation

## Useful Snippets

### Rename files 1,2,3 to 001,002,003:
```bash
# create dummy files
scr seq=10 sh="touch {}.txt"

# rename
ls -1 | scr lines r="\d+" [ exec mv {}.txt {:02}.txt ]
```
