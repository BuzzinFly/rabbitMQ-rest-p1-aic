import json, pathlib, sys

infile = sys.argv[1]
name = sys.argv[2]
outfile = sys.argv[3]

script = pathlib.Path(infile).read_text(encoding="utf-8")
payload = {
  "name": name,
  "type": "text/javascript",
  "context": "provisioning",
  "source": script
}
pathlib.Path(outfile).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
print(f"Wrote {outfile}")
