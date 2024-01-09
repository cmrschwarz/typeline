// day1:
// TODO: `foreach`, `to-int`
// oneliner: in l fe! r-m="\d" fc! h n t e j toi e sum p
// stdin lines foreach!
//     regex-m="\d" forkcat!
//         head
//     next
//         tail
//     end join toint
// end sum print

// day2:
// TODO: `compute`, `split`, `min`, `filter`,
// v@limits="{red: 12, green: 13, blue: 14}"
// stdin lines foreach!
//     regex="Game (?<game_id>\d+): (?<>.*)" split=";" foreach!
//          split=, regex="(?<n>\d+) (?<color>)" compute="limits[color]>=n"
//     end min@possible
// end filter=possible select=game_id toint sum print

// day3
// TODO: `collect`, `forkjoin`, `atoms`, `max`, `seq` (compute for provoking element)
// stdin lines enum@line_nr fj
//     collect atom@rows
// next
//     regex-m@num="\d+"
// end foreach!
//    seq@col="min(num.begin-1,0),max(num.end+1,len(rows[line_nr]))" foreach!
//        seq@row="min(line_nr-1,0),max(line_nr+2,len(rows))"
//        c='rows[row][col]!~="\d|."'
//    end max@is_part
// end filter=is_part sum print

// day4
// TODO: `contains` and `**` inside of `compute`
// stdin lines r=":.*" foreach!
//     regex="(?<>.*)|(?<have>)" split=" " collect@winning
//     select=have split=" " compute="contains(winning, _)" sum
//     compute="min(_, 1) * 2 ** max(_ - 1, 0)"
// end sum print

// day5
// TODO: scaneach
// v={dest_id=0,src_id:1,len_id:2} explode
// stdin regex-dm=".*?:(?<>.*?)\n[\n|$]" trim forkjoin!
//     tail=+1 foreach!
//         lines foreach! split=" " collect end collect atom@maps
//     end
// next
//    head split=" "
// end foreach!@seed key=loc #TODO: make sequence DUP and produce output for each record
//     seq@group_id="len(maps)" scaneach!
//         seq@rule_id="len(maps[group_id])"
//         compute@start="maps[group_id][rule_id][dest_id]"
//         compute@stop="start + maps[group_id][rule_id][len_id]"
//         filter="start <= _ && stop > _" +select=loc head@loc
//     end
// end min select@seed p
