~/workspace/maelstrom/pkg/maelstrom test -w echo --bin target/debug/echo --node-count 1 --time-limit 10
~/workspace/maelstrom/pkg/maelstrom test -w unique-ids --bin target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
~/workspace/maelstrom/pkg/maelstrom test -w broadcast --bin target/debug/broadcast --time-limit 20 --node-count 1 --rate 10
~/workspace/maelstrom/pkg/maelstrom test -w broadcast --bin target/debug/broadcast --time-limit 20 --node-count 5 --rate 10
~/workspace/maelstrom/pkg/maelstrom test -w broadcast --bin target/debug/broadcast --time-limit 20 --node-count 5 --rate 10 --nemesis partition
