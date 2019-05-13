# -*- encoding: utf-8 -*-
'''
Created on 2018/1/30
@author: liuyj

集群发布封装
'''

import os
import shutil
import tarfile
import traceback
import time
import json
from src.server_inm.base_inm.base_ansible_api import ans_shell, ans_copy, ans_script
from src.server_inm.base_inm.base_api_oper import get_cluster_info, download_nconf_lastversion, up_nginx_state, \
    get_cluster_nginx, get_kylinpacks, token_tmp_dir
from src.server_inm.base_inm.base_api_oper import add_zabbix_maintainance, del_zabbix_maintainance, push_deploy_info, \
    get_wk_pod
from src.server_inm.config_inm import global_config
from src.server_inm.mysql_inm.mysql_data import select_one_mysql
from src.server_inm.mysql_inm.mysql_data import add_one_mysql
from src.server_inm.mysql_inm.mysql_data import update_mysql
from src.server_inm.base_inm.base_api_oper import publish
from src.server_inm.base_inm.base_api_oper import update_consul
from src.server_inm.logs_inm.log import kylin_release
from src.server_inm.celery_inm.cluster_data_task import update_im_state
from src.server_inm.celery_inm.cluster_check_sso import parse_cookie


class Release:
    def __init__(self, ip, port, cookie, version=None, type=None):
        self.ip = ip
        self.port = port
        self.type = type
        self.version = version
        self.cookie = cookie
        self.script_path = global_config.SCRIPT_PATH
        self.download_path = "/opt/conf_d/"
        self.is_release = False
        self.is_start = False
        self.is_stop = False
        self.user = parse_cookie(cookie)
        self._release_info()
        self.ftp_host = global_config.FTP_HOST
        self.message = {
            'onlinename': self.onlinename,
            'ip': ip,
            'step': 'waiting...',
            'env': self.env,
            'type': type,
            'version': version
        }
        self.release_dict = {
            'tomcat': {
                'stop_command': 'sudo su - appuser -c "cd /apps/%s;sh ./restart_tomcat.sh stop"' % self.onlinename,
                'start_command': 'sudo su - appuser -c "cd /apps/%s;sh ./restart_tomcat.sh start"' % self.onlinename,
                'codeup_script': '%sappupdate.sh %s %s %s' % (
                    self.script_path, self.onlinename, self.appname, self.version),
                'codeup_script_qa': '%sappupdate_qa.sh %s %s %s %s %s' % (
                    self.script_path, self.onlinename, self.appname, self.packname, self.pack_prefix, self.pack_suffix)
            },
            'other': {
                'stop_command': 'test -f /apps/%s/stop.sh || exit 0 && sudo su - appuser -c "cd /apps/%s;sh ./stop.sh stop"' % (
                    self.onlinename, self.onlinename),
                'start_command': 'sudo su - appuser -c "cd /apps/%s; sh ./start.sh"' % self.onlinename,
                'codeup_script': '%sappupdate_dubbo.sh %s %s' % (self.script_path, self.onlinename, self.version),
                'codeup_script_qa': '%sappupdate_dubbo_qa.sh %s %s %s %s' % (
                    self.script_path, self.onlinename, self.packname, self.pack_prefix, self.pack_suffix)
            },
            'jar': {
                'stop_command': 'test -f /apps/%s/stop.sh || exit 0 && sudo su - appuser -c "cd /apps/%s;sh ./stop.sh stop"' % (
                    self.onlinename, self.onlinename),
                'start_command': 'sudo su - appuser -c "cd /apps/%s; sh ./start.sh"' % self.onlinename,
                'codeup_script': '%sappupdate_dubbo.sh %s %s' % (self.script_path, self.onlinename, self.version),
                'codeup_script_qa': '%sappupdate_dubbo_qa.sh %s %s %s %s' % (
                    self.script_path, self.onlinename, self.packname, self.pack_prefix, self.pack_suffix)
            }
        }
        self.params = {'IP': ip, 'Port': port}
        self.oper_data = {'oper_user': self.user, 'oper_type': type, 'oper_cluster': self.onlinename,
                          'oper_params': json.dumps(self.params), 'oper_status': ''}
        self._is_start()

        if version:
            self._is_release()

    def _publish(self, step=None, status=None, result=None):
        r"""对外发布消息，同时记录日志
            :param step: 操作步骤.
            :param status: True or False.
            :param result: 执行结果或者信息.
        """
        if step:
            self.message['step'] = step
            kylin_release.info(u"user %s oper %s of %s:%s step: %s, version: %s" % (
                self.user, self.message['type'], self.ip, self.port, step, self.version))
        if status is not None and result:
            self.message['status'] = status
            self.message['result'] = result
            if self.type != 'release':
                self.oper_data['oper_status'] = json.dumps((status, result))
                add_one_mysql('kylin_oper', self.oper_data)
            if not status:
                if self.type == 'release':
                    del_zabbix_maintainance(self.ip, port=self.port)
                    self._add_online_log(self.onlinename, self.version, self.ip, self.port, self.user, self.env,
                                         'Failure',
                                         result, self.cookie)
                kylin_release.error(u"user %s oper %s of %s:%s failure: %s, version %s" % (
                    self.user, self.message['type'], self.ip, self.port, result, self.version))
            else:
                if self.type == 'release':
                    self._add_online_log(self.onlinename, self.version, self.ip, self.port, self.user, self.env,
                                         'Success',
                                         result, self.cookie)
                kylin_release.info(u"user %s oper %s of %s:%s success: %s, version %s" % (
                    self.user, self.message['type'], self.ip, self.port, result, self.version))
        publish(self.message, type=self.onlinename, channel='release')

    # 记录上线版本历史
    def _add_online_log(self, online_name, im_version, im_ip, im_port, user, env, status, info, cookie):
        cluster_info = get_cluster_info(online_name, cookie)
        result = push_deploy_info(cluster_info['appname'], im_version, im_ip, im_port, env, user, status, info, cookie)
        if result.get('code') == 0:
            return True, 'add online record success'
        else:
            return False, 'add online record failure'

    def _release_info(self):
        # 临时目录用于储存配置文件
        if not os.path.isdir(self.download_path):
            os.makedirs(self.download_path)
        im_record = select_one_mysql('kylin_im', ['im_env', 'im_subcluster', 'im_servertype', 'im_state'],
                                     {'im_subip': self.ip, 'im_port': self.port})
        self.onlinename = im_record['im_subcluster']
        self.servertype = im_record['im_servertype']
        self.start_type = 'tomcat' if self.servertype == 'tomcat' else 'other'
        self.im_state = im_record['im_state']
        self.env = im_record['im_env']
        cluster_info = get_cluster_info(self.onlinename, self.cookie)
        if cluster_info:
            self.busline = cluster_info['busline']
            self.appname = cluster_info['appname']
            self.jappname = cluster_info['qaname']
        else:
            self.appname = '_'.join(self.onlinename.split('_')[:-1])

        kylinpack_status = get_kylinpacks(self.appname, self.version, cookies=self.cookie)
        if kylinpack_status.get('code', 1) == 0:
            kylinpack_info = kylinpack_status['data']
            self.conf_version = kylinpack_info[0]['conf_version']
            self.sdk_version = kylinpack_info[0]['sdk_version']
            self.ftp_user = kylinpack_info[0]['ftp_user']
            self.packname = kylinpack_info[0]['packname']
            self.opac_id = kylinpack_info[0]['opac_id']
            node = self.packname.rsplit('.', 1)
            self.pack_suffix = node[1]
            self.pack_prefix = node[0]
        else:
            self._publish(status=False, result=u'读取version数据失败')
            return False, 'read version failed'

    def _is_release(self):
        # 集群无任何状态时默认值为'-'
        if self.im_state == '-':
            self._publish(status=False, result=u'此IP未初始化!')
        else:
            self.is_release = True

    def _is_start(self):
        if self.im_state not in ['-']:
            self.is_start = True
        if self.im_state not in ['-']:
            self.is_stop = True

    # 将upstream文件同步到nginx服务器列表中
    def _up_upstream(self, state):
        nginx_status = up_nginx_state(self.ip, self.port, self.onlinename, state, self.cookie)
        return nginx_status

    def _upconf(self):
        try:
            if self.conf_version == 'no-conf':
                return True, 'no-conf'
            elif self.conf_version == '':
                return False, '此版本缺少配置信息，不允许在此生产环境部署!'
            self.dstdir = "/apps/%s/" % self.onlinename
            dir_status = token_tmp_dir(self.download_path, 'tmp_')
            if dir_status[0]:
                self.tmpdir = dir_status[1]
            else:
                return False, 'make tmp_dir error!'
            down_conf = self._downconf()
            if not down_conf[0]:
                self._clean_conf()
                return down_conf
            untar_conf = self._untar_conf()
            if not untar_conf[0]:
                self._clean_conf()
                return untar_conf
            scp_conf = self._scp_conf()
            self._clean_conf()
            return scp_conf
        except:
            self._clean_conf()
            self._publish(status=False, result=traceback.format_exc())
            return False, 'up conf error'
        pass

    def _downconf(self):
        # 部署pre,prt,pro环境的配置
        if self.conf_version and self.conf_version != 'no-conf':
            if self.env == 'prt':
                env_tmp = 'pro'
            else:
                env_tmp = self.env
            self.conf_filename = '%s_%s_%s_%s' % (self.jappname, env_tmp, 'master', self.conf_version)
            down_conf_path = self.tmpdir + self.conf_filename
            download_status = download_nconf_lastversion(self.jappname, env_tmp, 'master', self.conf_version,
                                                         down_conf_path)
            if download_status[0]:
                return True, 'conf download success!'
            else:
                self._publish(status=False, result=download_status[1])
                return False, 'download file error!'
        else:
            return True, 'no-conf'

    def _untar_conf(self):
        # 解压缩配置文件
        os.chdir(self.tmpdir)
        if tarfile.is_tarfile(self.conf_filename + '.tar.gz'):
            t = tarfile.open(self.conf_filename + '.tar.gz', "r:gz")
            t.extractall()
            t.close()
            return True, "Untar file succeed!"
        else:
            self._publish(status=False, result=u'下载%s配置已损坏!' % (self.env.upper()))
            return False, 'Untar conf file error'

    def _scp_conf(self):
        # 将指定环境的配置文件同步到远程服务器对应的目录下
        os.chdir(self.tmpdir)
        if os.path.isdir(self.conf_filename):
            conf_files = os.listdir(self.conf_filename)
            for file in conf_files:
                m = os.path.join(self.conf_filename, file)
                if os.path.isdir(m) and file == 'WEB-INF':
                    copy_command = "src=%s%s/WEB-INF/classes/ dest=%swebapps/%s/WEB-INF/classes/ force=yes owner=appuser group=appuser" % (
                        self.tmpdir, self.conf_filename, self.dstdir, self.appname)
                elif os.path.isdir(m):
                    copy_command = "src=%s%s/ dest=%s%s/ force=yes owner=appuser group=appuser" % (
                        self.tmpdir, m, self.dstdir, file)
                elif os.path.isfile(m):
                    copy_command = "src=%s%s dest=%s/ force=yes owner=appuser group=appuser" % (
                        self.tmpdir, m, self.dstdir)
                else:
                    copy_command = ''
                copy_status = ans_copy(self.ip, copy_command)
                if not copy_status[0]:
                    self._publish(status=False, result=copy_status[1])
                    return copy_status
        else:
            return False, 'pre conf dir is not exists'
        return True, 'pre up conf success'

    def _clean_conf(self):
        os.chdir('/')
        if hasattr(self, 'tmpdir') and os.path.isdir(self.tmpdir):
            shutil.rmtree(self.tmpdir)
            # os.remove(self.conf_filename + '.tar.gz')

    def _deltmp_tomcat(self):
        # 启动成功后删除/opt的中间目录
        deltmp_command = "rm -rf /opt/AppPac/%s/*" % self.onlinename
        deltmp_record = ans_shell(self.ip, deltmp_command)
        if deltmp_record[0]:
            return True, '/opt dir delete success'
        else:
            return False, deltmp_record[1]

    def _sdk_consul(self):
        if self.sdk_version != 'no-conf':
            sdk_status = update_consul(self.jappname, self.env, self.cookie, self.sdk_version)
            return sdk_status
        else:
            return True, 'no-conf'

    def _check_server(self, state, num=''):
        check_script = '%scheck_server.sh %s %s %s %s' % (
        self.script_path, self.onlinename, self.port, state, self.servertype)
        self._publish(step=u'检查%s %s...' % (state, num))
        check_status = ans_script(self.ip, check_script)
        return check_status

    def _check_start(self, state, count, sleep=5):
        check_status = True, ''
        for num in range(count):
            time.sleep(sleep)
            check_status = self._check_server(state, '(%s)' % str(num))
            if check_status[0]:
                break

        if not check_status[0]:
            self._publish(status=check_status[0], result=check_status[1])

        return check_status

    def stop_server(self):
        if not self.is_stop:
            self._publish(status=False, result=u'服务不允许停止！')
            return False, 'not allowed to stop'
        try:
            add_zabbix_maintainance(self.ip, port=self.port)

            self._publish(step=u'Nginx摘除...')
            nginx_result = self._up_upstream(0)
            if not nginx_result[0]:
                self._publish(status=False, result=nginx_result[1])
                return nginx_result

            self._publish(step=u'停止服务...')
            stop_status = ans_shell(self.ip, self.release_dict[self.start_type]['stop_command'])
            if not stop_status[0]:
                self._publish(status=stop_status[0], result=u'停止服务失败: %s!' % stop_status[1])
                return stop_status

            stop_status = self._check_start('stop', 10, 1)
            if not stop_status[0]:
                return stop_status

            update_im_state(self.ip, self.port, 'down')
            self.is_start = True
            if self.message['type'] == 'stop':
                self._publish(status=stop_status[0], result=stop_status[1])

            return True, '服务停止成功!'
        except:
            self._publish(status=False, result=traceback.format_exc())
            return False, 'stop server error!'

    def start_server(self):
        try:
            if not self.is_start:
                self._publish(status=False, result=u'服务不允许启动!')
                return False, 'not allowed to start'
            # 启动服务并检测服务启动成功
            self._publish(step=u'启动服务...')
            start_status = ans_shell(self.ip, self.release_dict[self.start_type]['start_command'])
            if not start_status[0]:
                self._publish(status=False, result=u'启动服务失败!')
                return start_status

            # 检测进程
            start_status = self._check_start('start', 5, 2)
            if not start_status[0]:
                return start_status

            # 检测端口, 若服务不检测端口则设置端口为'0'
            if self.port != '0':
                port_status = self._check_start('port', 24, 3)
                if not port_status[0]:
                    return port_status

            self._publish(step=u'挂载nginx')
            nginx_result = self._up_upstream(1)
            update_im_state(self.ip, self.port, 'running')
            self.is_stop = True
            if not nginx_result[0]:
                self._publish(status=False, result=nginx_result[1])
                return nginx_result

            del_zabbix_maintainance(self.ip, port=self.port)

            self._publish(status=True, result=u'服务启动成功!')
            return True, 'start server success'
        except:
            self._publish(status=False, result=traceback.format_exc())
            return False, 'start server error!'

    def codeup_server(self):
        # FTP 下载指定版本的代码包
        try:
            # 推consul有问题
            sdk_status = self._sdk_consul()
            if not sdk_status[0]:
                self._publish(status=False, result=sdk_status[1])
                return sdk_status
            # 如果有conf配置版本，直接使用新的部署方式进行部署
            if self.conf_version:
                self._publish(step=u'下载QA包...')
                ftp_user = self.ftp_user
                ftp_pwd = global_config.FTP_ACCOUNT[ftp_user]
                ftp_command = '%sftpget_qa.sh %s %s %s %s %s %s' % (
                    global_config.SCRIPT_PATH, self.ftp_host, self.onlinename, ftp_user, ftp_pwd, self.jappname,
                    self.packname)
                ftp_record = ans_script(self.ip, ftp_command)
                if ftp_record[0]:
                    # 更新代码包
                    self._publish(step=u'更新QA包...')
                    codeup_status = ans_script(self.ip, self.release_dict[self.start_type]['codeup_script_qa'])
                    self._deltmp_tomcat()
                    if codeup_status[0]:
                        tmp = u'更新%s配置...' % (self.env.upper())
                        self._publish(step=tmp)
                        codeup_status = self._upconf()

                    if not codeup_status[0]:
                        self._publish(status=False, result=codeup_status[1])
                        return codeup_status

                    update_status = update_mysql('kylin_im', {'im_version': self.version},
                                                 {'im_subip': self.ip, 'im_port': self.port})
                    if isinstance(update_status, bool):
                        self._publish(status=False, result=u'更新数据插入数据库错误!')
                    return codeup_status
                else:
                    self._publish(status=False, result=u'下载包失败!')
                    self._deltmp_tomcat()
                    return False, ftp_record[1]
            # 如果没有conf配置版本，则通过老部署方式进行部署，是一种过渡方案
            else:
                if self.env == 'pro':
                    self._publish(step=u'下载上线包...')
                    ftp_command = '%sftpget.sh %s %s %s' % (
                        global_config.SCRIPT_PATH, self.ftp_host, self.onlinename, self.version)
                    ftp_record = ans_script(self.ip, ftp_command)
                    if ftp_record[0]:
                        # 更新代码包
                        self._publish(step=u'更新上线包...')
                        codeup_status = ans_script(self.ip, self.release_dict[self.start_type]['codeup_script'])
                        self._deltmp_tomcat()
                        # if codeup_status[0] and self.env == 'pre':
                        #     self._publish(step=u'更新PRE配置...')
                        #     codeup_status = self._pre_upconf()

                        if not codeup_status[0]:
                            self._publish(status=False, result=codeup_status[1])
                            return codeup_status

                        update_mysql('kylin_im', {'im_version': self.version},
                                     {'im_subip': self.ip, 'im_port': self.port})
                        return codeup_status
                    else:
                        self._publish(status=False, result=u'下载包失败!')
                        self._deltmp_tomcat()
                        return False, ftp_record[1]
                else:
                    self._publish(status=False, result=u'使用旧版kylin打出的包，只能在新版kylin的PRO坏境部署!')
                    return False, u'使用旧版kylin打出的包，只能在新版kylin的PRO坏境部署!'

        except:
            self._publish(status=False, result=traceback.format_exc())
            return False, 'codeup server error!'


if __name__ == "__main__":
    pass
