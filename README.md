#虚谷数据库使用JDBC的demo程序

1、GetConnection 类通过JDBC和虚谷数据库建立连接。连接方式包含了连接参数方式、数据源方式和多IP方式。
2、StatementTest 测试类实现了DDL语句、DML语句的执行，对多结果集的处理，对GeneratedKey的处理。
3、PreparedStatement 测试类实现了对DDL语句测执行，对LOB对象的操作，批量插入数据和对GeneratedKey的处理。
4、CallableStatementTest 测试类实现了对存储过程（返回引用游标）、存储函数的调用（返回值），对带名参数的设置等处理。
5、target/lib为虚谷JDBC驱动