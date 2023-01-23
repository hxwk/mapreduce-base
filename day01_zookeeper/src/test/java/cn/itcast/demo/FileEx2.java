package cn.itcast.demo;

import java.io.*;

//统计一篇文章中出现的指定字符的个数
public class FileEx2 {
    public static void main(String[] args) throws IOException {
        InputStreamReader fis = new InputStreamReader(new FileInputStream("D:\\project\\Shanghai26\\day01_zookeeper\\data\\source.txt"), "UTF8");
        BufferedReader br=new BufferedReader(fis);
        String s="";
        char c='嫦';
        StringBuilder sb=new StringBuilder();
        while ((s=br.readLine())!=null){
            sb.append(s);
        }
        br.close();
        System.out.println("1234:"+sb);

        String s1 = sb.toString();
        int count=0;
        for (int i = 0; i < s1.length(); i++) {
            if (s1.charAt(i)==c){
                System.out.println(c);
                count++;
            }
        }
        System.out.println("嫦出现的次数为:"+count);
    }
}