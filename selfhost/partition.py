import json, collections
items=[json.loads(l) for l in open('/data/todo_top.jsonl') if l.strip()]
byaid=collections.defaultdict(list)
for it in items: byaid[it['aid']].append(it)
aids=sorted(byaid)
parts=[[],[],[]]
# round-robin anime across 3 nodes (balances anime count; episodes roughly follow)
for i,aid in enumerate(aids): parts[i%3].extend(byaid[aid])
for i,p in enumerate(parts):
    open(f'/data/todo_n{i+1}.jsonl','w').writelines(json.dumps(x)+'\n' for x in p)
print('partitioned %d anime / %d items -> n1=%d n2=%d n3=%d'%(len(aids),len(items),len(parts[0]),len(parts[1]),len(parts[2])))
