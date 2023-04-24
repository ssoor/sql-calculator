#!/bin/sh
set -e

# 用压缩软件打开 mwb 文件，并用里面的 document.mwb.xml 覆盖项目下的 document.mwb.xml

# 环境安装
# wget https://dev.mysql.com/get/Downloads/MySQLGUITools/mysql-workbench-community_8.0.27-1ubuntu20.04_amd64.deb
# apt install ./mysql-workbench-community_8.0.27-1ubuntu20.04_amd64.deb

# apt install xvfb
# git clone https://github.com/ssoor/sql-calculator && cd sql-calculator && go install .

export IN=${IN:-'./document.mwb.xml'}
export OUT=${OUT:-'./document.mwb.sql'}

export MYSQL_DSN=${MYSQL_DSN:-'root:root@tcp(mysql:3306)/mysql'}
export TABLE_FILTER_LIST=${TABLE_FILTER_LIST:-''} # _table1,_table2

rm -f "${OUT}"
rm -rf ~/.mysql/workbench/

xvfb-run --auto-servernum --server-args='-screen 0 640x480x24' mysql-workbench --quit-when-done --run-script ./er2sql.py >/dev/null

# 获取差异

sql-calculator dump "${MYSQL_DSN}" > ./source.sql
sql-calculator diff  ./source.sql ./document.mwb.sql
