// day1:
// lines fe: r-m="\d" fc: head next tail end join to_int end sum

// day2:
// TODO: `c`, `split`, `min`, `filter`,
// %limits="{red: 12, green: 13, blue: 14}"
// lines fe:
//     r="Game (?<game_id>\d+): (?<>.*)" split=";" fe:
//          split=, r="(?<n>\d+) (?<color>)" c="limits[color]>=n"
//     end min@possible
// end filter=possible select=game_id toint sum print

// day3
// TODO: `forkjoin`, `atoms`, `max`, `seq` (c for provoking
// element) lines enum@line_nr fj
//     collect atom@rows
// next
//     r-m@num="\d+"
// end fe:
//    seq@col="min(num.begin-1,0),max(num.end+1,len(rows[line_nr]))" fe:
//        seq@row="min(line_nr-1,0),max(line_nr+2,len(rows))"
//        c='rows[row][col]!~="\d|."'
//    end max@is_part
// end filter=is_part sum print

// day4
// TODO: `contains` and `**` inside of `c`
// lines r=":.*" fe:
//     r="(?<>.*)|(?<have>)" split=" " collect@winning
//     select=have split=" " c="contains(winning, _)" sum
//     c="min(_, 1) * 2 ** max(_ - 1, 0)"
// end sum print

// day5
// TODO: scaneach
// v={dest_id=0,src_id:1,len_id:2} explode
// r-dm=".*?:(?<>.*?)\n[\n|$]" trim forkjoin!
//     tail=+1 fe:
//         lines fe: split=" " collect end collect atom@maps
//     end
// next
//    head split=" "
// end fe:@seed key=loc #TODO: make sequence DUP and produce output for
// each record     seq@group_id="len(maps)" scaneach!
//         seq@rule_id="len(maps[group_id])"
//         c@start="maps[group_id][rule_id][dest_id]"
//         c@stop="start + maps[group_id][rule_id][len_id]"
//         filter="start <= _ && stop > _" +select=loc head@loc
//     end
// end min select@seed p
