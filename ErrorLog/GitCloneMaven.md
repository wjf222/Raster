# 错误：git 项目后无法找到主函数
错误截图：
![无法加载主类](Pictures/无法加载主类.png)

原因：
![没有sourceFolder](Pictures/没有sourceFolder.png)
是因为在setting中没有设置sourceFolders

解决方法：
打开projectStructure,选择module->source
![projectStruct.png](Pictures/projectStruct.png)

选择需要添加的目录位sourceFolder
![img.png](Pictures/sourceFolderAdd.png)