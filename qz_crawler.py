import uuid

import pymongo

from qz_util import *


def crawl_emotion_by_uin(dc: QZDataCatcher, uin: str) -> None:
    log('用户uin=%s开始爬取' % uin)
    # 打开数据库连接
    mongo_clt = pymongo.MongoClient()
    mongo_db = mongo_clt['qz_crawler']
    mongo_col = mongo_db[uin]
    log('数据库已连接')
    ''' dc实例由调用者提供
    # 初始化数据捕手
    dc = QZDataCatcher()
    dc.auth()  # 自己有log显示
    '''
    # 说说列表主遍历
    emotion_list_pos = 0
    while True:
        # 请求list接口
        emotion_list = dc.request_emotion_list(uin, emotion_list_pos, 20)
        # 检查状态码
        # 如果不存在code和subcode，后续也会有其他错误，直接用[]
        if emotion_list['code'] != 0 or emotion_list['subcode'] != 0:
            # 已经报错了，就保守点
            raise StatusCodeError(emotion_list.get('message', '<KeyError while reading message from response>'),
                                  emotion_list['code'], emotion_list['subcode'])
        # 检查是否爬取完毕
        # msglist一定存在，但可能是None，故进行类型检查
        if not (isinstance(emotion_list['msglist'], list) and len(emotion_list['msglist']) > 0):
            break
        # 说说一级遍历
        for pre_mapping_doc in emotion_list['msglist']:
            assert isinstance(pre_mapping_doc, dict)  # assert以防万一
            assert isinstance(pre_mapping_doc.get('tid'), str) and len(pre_mapping_doc['tid']) > 0
            # 检查是否已经爬过了
            if mongo_col.find_one({'tid': pre_mapping_doc['tid']}) is not None:
                log('说说tid=%s已经存在于数据库中，跳过！' % pre_mapping_doc['tid'])
                continue
            else:
                log('说说tid=%s开始爬取……' % pre_mapping_doc['tid'])
            # 检查是否需要请求detail接口
            # 两个more_con可能不存在，cmtnum和commentlist一定存在，但为保持一致性都用get
            # commentlist可能是None，故进行类型检查
            if pre_mapping_doc.get('has_more_con', 0) == 1 or \
                    pre_mapping_doc.get('rt_has_more_con', 0) == 1 or \
                    pre_mapping_doc.get('cmtnum', 0) > \
                    (len(pre_mapping_doc.get('commentlist')) if isinstance(pre_mapping_doc.get('commentlist'),
                                                                           list) else 0):
                # 如果不存在tid，后续也会有其他错误，直接用[]
                pre_mapping_doc = dc.request_emotion_detail(uin, pre_mapping_doc['tid'], 0, 20)
                # 检查状态码
                # code和subcode必须存在，故用[]
                if pre_mapping_doc['code'] != 0 or pre_mapping_doc['subcode'] != 0:
                    # 已经报错了，就保守点
                    raise StatusCodeError(emotion_list.get('message', '<KeyError while reading message from response>'),
                                          emotion_list['code'], emotion_list['subcode'])
            # 映射基本数据
            # 部分数据一定存在，部分数据可能不存在，为保持一致性都用get
            # 需要稍后获取的数据全部先设为对应类型的默认值，即使稍后没有获取到数据，类型依然是正确的
            post_mapping_doc = {
                'tid': pre_mapping_doc.get('tid', ''),
                'uin': pre_mapping_doc.get('uin', 0),
                'nickname': pre_mapping_doc.get('name', ''),
                'content': pre_mapping_doc.get('content', ''),
                'timestamp': pre_mapping_doc.get('created_time', 0),
                'create_time': pre_mapping_doc.get('createTime', ''),
                'device_name': pre_mapping_doc.get('source_name', ''),
                'rt_tid': pre_mapping_doc.get('rt_tid', ''),
                'rt_uin': pre_mapping_doc.get('rt_uin', 0),
                'rt_nickname': pre_mapping_doc.get('rt_uinname', ''),
                'rt_content': '',  # 折叠结构，稍后获取
                'rt_create_time': pre_mapping_doc.get('rt_createTime', ''),
                'rt_device_name': pre_mapping_doc.get('rt_source_name', ''),
                'rt_comment_num': pre_mapping_doc.get('rt_cmtnum', 0),
                'rt_forward_num': pre_mapping_doc.get('rt_fwdnum', 0),
                'rt_forward_list': [],  # 折叠结构，稍后获取
                'location': {},  # 折叠结构，稍后获取
                'comment': [],  # 折叠结构，稍后获取
                'pic': [],  # 可能需要请求get_pic接口，而且需要下载，稍后获取
                'like': [],  # 独立接口，稍后获取
                'do_like': 0  # 独立接口，稍后获取
            }
            # 映射rt_content
            # 可能不存在rt_con，故用get
            if isinstance(pre_mapping_doc.get('rt_con'), dict):
                # 虽然content一定存在，但为了代码统一都用get
                # rt_con已经判定为dict，也就是说一定存在，直接用[]
                post_mapping_doc['rt_content'] = pre_mapping_doc['rt_con'].get('content', '')
            # 映射rt_forward_list
            # 可能不存在rtlist，先get再检查
            if isinstance(pre_mapping_doc.get('rtlist'), list):
                for pre_rt_fwd_doc in pre_mapping_doc['rtlist']:
                    assert isinstance(pre_rt_fwd_doc, dict)
                    post_mapping_doc['rt_forward_list'].append({
                        'tid': pre_rt_fwd_doc.get('tid', ''),
                        'uin': pre_rt_fwd_doc.get('uin', 0),
                        'nickname': pre_rt_fwd_doc.get('name', ''),
                        'content': pre_rt_fwd_doc.get('con', ''),
                        'create_time': pre_rt_fwd_doc.get('time', ''),
                        'device_name': pre_rt_fwd_doc.get('source_name', '')
                    })
            # 映射location
            # 先指向常规lbs，lbs一定存在，但为了代码统一用get
            pre_location_doc = pre_mapping_doc.get('lbs')
            # 如果存在story_info中的lbs，再覆盖，story_info可能不存在，需要检查
            if isinstance(pre_mapping_doc.get('story_info'), dict):
                pre_location_doc = pre_mapping_doc['story_info'].get('lbs')
            # 类型检查，保证数据完整性，用assert来显示类型不匹配的情况
            assert isinstance(pre_location_doc, dict)
            # location详细数据映射
            post_mapping_doc['location'] = {
                'id': pre_location_doc.get('id', ''),
                'id_name': pre_location_doc.get('idname', ''),
                'name': pre_location_doc.get('name', ''),
                'pos_x': pre_location_doc.get('pos_x', ''),  # 没错这是string，企鹅工程师可能是为了保持数据精度
                'pos_y': pre_location_doc.get('pos_y', '')  # 没错这也是string
            }
            # 映射comment
            # commentlist一定存在，但可能是None，故进行类型检查
            if isinstance(pre_mapping_doc.get('commentlist'), list):
                # 评论二级遍历
                for pre_cmt_doc in pre_mapping_doc.get('commentlist'):
                    assert isinstance(pre_cmt_doc, dict)
                    # 映射评论基本数据
                    post_cmt_doc = {
                        'tid': pre_cmt_doc.get('tid', 0),
                        'uin': pre_cmt_doc.get('uin', 0),
                        'nickname': pre_cmt_doc.get('name', ''),
                        'content': pre_cmt_doc.get('content', ''),
                        'timestamp': pre_cmt_doc.get('create_time', 0),
                        'create_time': pre_cmt_doc.get('createTime2', ''),
                        'sub_comment': [],  # 折叠数据，稍后获取
                        'pic': []  # 独立接口，稍后获取
                    }
                    # 映射sub_comment
                    # list_3可能不存在，所以用get结合类型检查
                    if isinstance(pre_cmt_doc.get('list_3'), list):
                        # 子评论三级遍历
                        for pre_sub_cmt_doc in pre_cmt_doc['list_3']:
                            assert isinstance(pre_sub_cmt_doc, dict)
                            # 映射子评论基本数据
                            post_sub_cmt_doc = {
                                'tid': pre_sub_cmt_doc.get('tid', 0),
                                'uin': pre_sub_cmt_doc.get('uin', 0),
                                'nickname': pre_sub_cmt_doc.get('name', ''),
                                'content': pre_sub_cmt_doc.get('content', ''),
                                'timestamp': pre_sub_cmt_doc.get('create_time', 0),
                                'create_time': pre_sub_cmt_doc.get('createTime2', '')
                            }
                            # 保存单条子评论
                            post_cmt_doc['sub_comment'].append(post_sub_cmt_doc)
                    # 映射评论pic
                    # pic可能不存在，所以用get结合类型检查
                    if isinstance(pre_cmt_doc.get('pic'), list):
                        for pic_doc in pre_cmt_doc['pic']:
                            assert isinstance(pic_doc, dict)  # assert以防万一
                            # o_url一定存在，为保持一致用get
                            url = pic_doc.get('o_url')
                            # 为了保证爬取的数据的完整性，用assert确保url是合法的
                            assert isinstance(url, str) and len(url) > 0
                            file_uuid = uuid.uuid5(uuid.NAMESPACE_URL, url).hex
                            # 如果数据文件夹不存在，则创建
                            if not os.path.exists('crawler_pic'):
                                os.mkdir('crawler_pic')
                            # 如果对应用户的文件夹不存在，则创建
                            if not os.path.exists('crawler_pic/%s' % uin):
                                os.mkdir('crawler_pic/%s' % uin)
                            # 保存图片
                            ret = dc.save_as_file(url, 'crawler_pic/%s/%s.jpg' % (uin, file_uuid))
                            # 保存图片索引
                            post_cmt_doc['pic'].append({
                                'pic_original_url': url,
                                'pic_local_name': file_uuid if ret else ''
                            })
                    # 保存单条评论
                    post_mapping_doc['comment'].append(post_cmt_doc)
            # 映射pic
            # pic可能不存在，get加类型检查
            if isinstance(pre_mapping_doc.get('pic'), list):
                # 填充url列表
                pic_url_list = []
                # 检查pic列表是否完整
                if isinstance(pre_mapping_doc.get('pictotal'), int) and \
                        pre_mapping_doc['pictotal'] > len(pre_mapping_doc['pic']):
                    # 照片列表不完全，请求get_pics接口
                    pic_list = dc.request_pics_list(uin, pre_mapping_doc['tid'])
                    # 检查状态码
                    assert isinstance(pic_list.get('result'), dict)
                    if pic_list['result']['code'] != 0:
                        raise StatusCodeError(
                            pic_list['result'].get('msg', '<KeyError while reading message from response>'),
                            pic_list['code'])
                    assert isinstance(pic_list.get('images'), list)
                    for pic_doc in pic_list['images']:
                        assert isinstance(pic_doc, dict)
                        p_url = pic_doc.get('big_pic')
                        assert isinstance(p_url, str) and len(p_url) > 0
                        pic_url_list.append(p_url)
                else:
                    # pic已经查过了，一定是list
                    for pic_doc in pre_mapping_doc['pic']:
                        assert isinstance(pic_doc, dict)  # assert以防万一
                        # url3一定存在，为保持一致用get
                        p_url = pic_doc.get('url3')
                        # 保证数据完整
                        assert isinstance(p_url, str) and len(p_url) > 0
                        pic_url_list.append(p_url)
                # 下载url列表中的图片
                for p_url in pic_url_list:
                    file_uuid = uuid.uuid5(uuid.NAMESPACE_URL, p_url).hex
                    # 如果数据文件夹不存在，则创建
                    if not os.path.exists('crawler_pic'):
                        os.mkdir('crawler_pic')
                    # 如果对应用户的文件夹不存在，则创建
                    if not os.path.exists('crawler_pic/%s' % uin):
                        os.mkdir('crawler_pic/%s' % uin)
                    # 保存图片
                    ret = dc.save_as_file(p_url, 'crawler_pic/%s/%s.jpg' % (uin, file_uuid))
                    # 保存图片索引
                    post_mapping_doc['pic'].append({
                        'pic_original_url': p_url,
                        'pic_local_name': file_uuid if ret else ''
                    })
            # 映射like
            # 接口变量：记录最后一个uin
            like_begin_uin = '0'
            # 赞列表循环
            while True:
                # tid必须存在
                like_list = dc.request_like_list(uin, pre_mapping_doc['tid'], like_begin_uin)
                # 检查状态码，同样的，code和subcode必须存在
                if like_list['code'] != 0 and like_list['subcode'] != 0:
                    # 已经报错了，就保守点
                    raise StatusCodeError(like_list.get('message', '<KeyError while reading message from response>'),
                                          like_list['code'], like_list['subcode'])
                # 检查赞信息是否已经爬取完毕
                assert isinstance(like_list.get('data'), dict)  # assert以防万一
                assert isinstance(like_list['data'].get('like_uin_info'), list)  # assert以防万一
                if len(like_list['data']['like_uin_info']) <= 0:
                    break
                # 获取do_like信息
                # is_dolike可能不存在，需要检查，不存在就不去修改记录中的数据
                if isinstance(like_list['data'].get('is_dolike'), int):
                    post_mapping_doc['do_like'] = like_list['data']['is_dolike']
                # 赞用户二级循环
                for pre_like_doc in like_list['data']['like_uin_info']:
                    assert isinstance(pre_like_doc, dict)
                    post_mapping_doc['like'].append({
                        'uin': pre_like_doc.get('fuin', 0),
                        'nickname': pre_like_doc.get('nick', ''),
                        # 'portrait_url': pre_like_doc.get('portrait', ''),
                        'gender': pre_like_doc.get('gender', ''),
                        'constellation': pre_like_doc.get('constellation', ''),
                        'address': pre_like_doc.get('addr', '')
                    })
                # 更新like_begin_uin
                # 可以确定like_uin_info是非空list，也可以确定最后一项是dict
                # 为了数据完整性，不接受fuin不存在的情况，用[]
                like_begin_uin = like_list['data']['like_uin_info'][-1]['fuin']
            # 保存单条说说到数据库
            mongo_col.insert_one(post_mapping_doc)
            # 单条说说日志
            log('说说tid=%s爬取完毕！\t图%s\t评%s\t赞%s' % (
                post_mapping_doc['tid'],
                len(post_mapping_doc['pic']),
                len(post_mapping_doc['comment']),
                len(post_mapping_doc['like'])
            ))
        # 增大pos参数
        emotion_list_pos += 20
    # 关闭数据库连接
    mongo_clt.close()
    log('用户uin=%s爬取完毕，断开数据库连接' % uin)
