`Usage: python repl.py`

Commands:

```
!<shell_command>
cd [<db>]
ls [<db>]
rm <db|collection>
cp <src> <dst>
mv <src> <dst>
cat <collection> [ > <file> ]
less <collection>
exit
```
This is a very basic mongo shell (see the above commands).
The current code assumes that mongodb runs on local host
using the default port, and that there is no authentication.
