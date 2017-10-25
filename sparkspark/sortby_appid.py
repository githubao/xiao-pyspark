#!/usr/bin/env python
# encoding: utf-8

"""
@description: 数据分析的操作

@author: pacman
@time: 2017/10/25 17:11
"""

from collections import defaultdict

root_path = 'C:\\Users\\BaoQiang\\Desktop\\'


def run():
    dic = defaultdict(dict)
    cate_dic = {}
    out_path = '{}/cate'.format(root_path)

    with open('{}/request.txt'.format(root_path), 'r', encoding='utf-8') as f, \
            open('{}/cate.txt'.format(root_path), 'w', encoding='utf-8') as fcate:
        for line in f:
            attr = line.strip().split('\t')
            if len(attr) != 4:
                continue

            ques, ans, appid, parsetype = attr

            key = '{}-{}'.format(appid, parsetype)
            value = '{}\t{}'.format(ques, ans)

            if value in dic[key]:
                dic[key][value] += 1
            else:
                dic[key][value] = 1

        for key, values in dic.items():
            with open('{}/{}.txt'.format(out_path, key), 'w', encoding='utf-8') as fw:
                total = 0
                for qa, cnt in values.items():
                    fw.write('{}\t{}\n'.format(qa, cnt))
                    total += cnt

                cate_dic[key] = total

        sorted_cate = sorted(cate_dic.items(), key=lambda x: x[1], reverse=True)

        for key, value in sorted_cate:
            fcate.write('{}\t{}\n'.format(key, value))


def run2():
    with open('{}/cate.txt'.format(root_path), 'r', encoding='utf-8') as f1, \
            open('{}/cate1.txt'.format(root_path), 'r', encoding='utf-8') as f2, \
            open('{}/result.txt'.format(root_path), 'w', encoding='utf-8') as fw:
        dic = {}

        for line in f2:
            appid, desc = line.strip().split('\t')
            dic[appid] = desc

        for line in f1:
            app, cnt = line.strip().split('\t')
            appid, parsetype = app.split('-')
            if appid in dic:
                res = dic[appid]
            else:
                res = ''

            fw.write('{}\t{}\t{}\n'.format(line.strip(),appid, res))


def main():
    # run()
    run2()


if __name__ == '__main__':
    main()
