package diff

import (
	"fmt"
	"testing"

	"github.com/ssoor/sql-calculator/utils"
)

func TestGetDiffFromSqlFile(t *testing.T) {
	sourceTable := `
	CREATE TABLE qk_t1 (
	  id CHAR(36) NOT NULL COMMENT '主键ID',
	  normal CHAR(36) NOT NULL DEFAULT '' COMMENT '字段注释',
	  will_be_change CHAR(36) NOT NULL DEFAULT '' COMMENT '会被修改的注释',
	  will_be_delete CHAR(36) NOT NULL DEFAULT '' COMMENT '被删除字段的注释',
	  PRIMARY KEY (id),
	  INDEX normal_idx (normal ASC),
	  INDEX will_be_change_idx (will_be_change ASC),
	  INDEX will_be_change2_idx (will_be_change ASC),
	  INDEX will_be_delete_idx (will_be_delete ASC));
	
	CREATE TABLE IF NOT EXISTS qk_t2 (
	  id INT(11) NOT NULL AUTO_INCREMENT,
	  old_relation_id CHAR(36) NULL DEFAULT NULL COMMENT '字段注释1',
	  new_relation_id CHAR(36) NULL DEFAULT NULL COMMENT '字段注释2',
	  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
	  INDEX new_relation_id_idx (new_relation_id ASC),
	  INDEX old_relation_id_idx (old_relation_id ASC),
	  PRIMARY KEY (id))
	COMMENT = '原来的注释';
	
`

	targetTable := `
	CREATE TABLE qk_t1 (
	  id CHAR(36) NOT NULL COMMENT '主键ID',
	  normal CHAR(36) NOT NULL DEFAULT '' COMMENT '字段注释',
	  will_be_change INT(11) NOT NULL DEFAULT '新的默认值' COMMENT '新的字段注释',
	  will_be_append CHAR(36) NOT NULL DEFAULT '' COMMENT '新增的字段',
	  PRIMARY KEY (normal),
	  INDEX normal_idx (normal ASC),
	  INDEX will_be_change_idx (will_be_change DESC),
	  INDEX will_be_change2_idx (normal ASC),
	  INDEX will_be_append_idx (will_be_append ASC));
	
	CREATE TABLE IF NOT EXISTS qk_t2 (
	  id INT(11) NOT NULL AUTO_INCREMENT,
	  old_relation_id CHAR(36) NULL DEFAULT NULL COMMENT '字段注释1',
	  new_relation_id CHAR(36) NULL DEFAULT NULL COMMENT '字段注释2',
	  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
	  INDEX new_relation_id_idx (new_relation_id ASC),
	  INDEX old_relation_id_idx (old_relation_id ASC),
	  PRIMARY KEY (id))
	COMMENT = '注释被修改';
`

	alters, err := GetDiffFromSqlFile("db1", sourceTable, targetTable)
	if err != nil {
		t.Error(err)
		return
	}
	for _, alter := range alters {
		sql, _ := utils.RestoreToSql(alter)
		fmt.Println(sql)
	}
}
