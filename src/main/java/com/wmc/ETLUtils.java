package com.wmc;

/**
 * @author: WangMC
 * @date: 2019/8/2 17:12
 * @description:
 */


/**
 * 1、过滤脏数据
 * 2、去掉 类别字段 中的空格
 * 3、替换关联视频的分隔符
 */
public class ETLUtils {

    public static String etlString(String line) {
        // 切割数据
        String[] split = line.split("\t");

        // 过于不满足全字段的脏数据
        if (split.length < 9) {
            return null;
        }
        // 去掉 类别字段 中的空格
        split[3] = split[3].replaceAll(" ", "");

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < split.length; i++) {
            // 前9个字段的拼接
            if (i < 9) {
                if (i == split.length - 1) {
                    sb.append(split[i]);
                } else {
                    sb.append(split[i]).append("\t");
                }
            }
            // 第10个字段的拼接
            else {
                if (i == split.length - 1) {
                    sb.append(split[i]);
                } else {
                    sb.append(split[i]).append("&");
                }
            }
        }
        return sb.toString();
    }
}
