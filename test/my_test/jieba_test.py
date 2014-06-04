# -*- coding: utf-8 -*-
import jieba

seg_list = jieba.cut("我工作的院里有一栋小楼，三层，有斑驳的外墙和爬山虎，还有运转不停的排风扇，门口的屏风刻着毛泽东语录，我很想有一天租下来改造成这个样子。")
print "Default Mode:", "/".join(seg_list)
