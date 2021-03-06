# Cuttle

![Build status](https://api.travis-ci.org/criteo/cuttle.svg?branch=master)

## Development

To work on the Scala API, run sbt using `sbt -DdevMode` (the devMode
option allows to skip webpack execution to speed up execution). You can
use `~compile` or `~test` on the root project or focus on a specific
project using `~timeseries/test`.

To run an example application, use `example HelloWorld` where HelloWorld
is the example class name.

To work on the web application:

- Install [**yarn**](https://yarnpkg.com/en/)
- Run, `yarn install` and `yarn start`.

### Local Database

To run the embedded MySQL on Linux you will need:
 - ncurses 5, if your distribution is already using ncurses 6 you can probably install a
compatibility package (e.g. ncurses5-compat-libs on Archlinux).
 - libaio1.so, if your distribution has not it installed by default (ex: Ubuntu):
   ```
   $> sudo apt install libaio1
   ```

### Format Javascript

Use `yarn format` to format the Javascript files.

### Scalafmt

We use Scalafmt to enforce style.  The minimal config is found in the
.scalafmt.conf file (you probably won't make any friends if you change
this).

To use you can install the [IntelliJ
plugin](http://scalameta.org/scalafmt/#IntelliJ) and use the familiar
shift-ctrl-L shortcut.

You can also use the sbt plugin:
```
$> sbt "scalafmt -i"
```
See also:
```
$> sbt "scalafmt -help"
```
You might also want to read details of the [sbt
integration](http://scalameta.org/scalafmt/#sbt).
