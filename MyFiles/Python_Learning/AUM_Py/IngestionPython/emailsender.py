from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailSender(object):
    def __init__(self, sc, config):
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger("IDDLLogger")
        self._sc = sc
        source = config.get('source')
        sub_source = config.get('sub_source')

        if sub_source:
            self._source = source + '_' + sub_source
        else:
            self._source = source

        if config.get('debug_email') == 'yes':
            self._debug = 'yes'
        else:
            self._debug = 'no'

        self.smtp_server = config.get('smtp_server')
        self.smtp_port = config.get('smtp_port')

        self.msg = MIMEMultipart()
        self.msg['From'] = config.get('mail_from')
        self.msg['To'] = config.get('mail_to')

        if config.get('mail_cc'):
            self.msg['Cc'] = config.get('mail_cc')
            self.receipts = self.msg['To'].split(',') + self.msg['Cc'].split(',')
        else:
            self.receipts = self.msg['To'].split(',')

    def send_detail_error_email(self, statusdict):
        server = SMTP(self.smtp_server, self.smtp_port)
        if self._debug == 'yes':
            server.set_debuglevel(1)

        server.starttls()

        self.msg['Subject'] = "Ingestion Job %s for %s failed" % (self._sc.applicationId, self._source)
        self.logger.error(self.msg['Subject'])
        msg_content = ''
        for k, v in statusdict.items():
            statustr = 'filepath = %s, status = %s' % (k, v)
            self.logger.info(statustr)
            msg_content = msg_content + statustr + '<br><br>'
        self.msg.attach(MIMEText(msg_content, 'html'))

        try:
            server.sendmail(self.msg['From'], self.receipts, self.msg.as_string())
            self.logger.info('email was sent to ' + ','.join(self.receipts))
        except Exception as e:
            self.logger.error('failed at sending email: ' + str(e))
        finally:
            server.quit()

    def send_error_email(self, msg):
        server = SMTP(self.smtp_server, self.smtp_port)
        if self._debug == 'yes':
            server.set_debuglevel(1)

        server.starttls()

        self.msg['Subject'] = "Ingestion Job %s for %s failed" % (self._sc.applicationId, self._source)
        self.logger.error(self.msg['Subject'])
        msg_content = msg
        self.msg.attach(MIMEText(msg_content, 'html'))

        try:
            server.sendmail(self.msg['From'], self.receipts, self.msg.as_string())
            self.logger.info('email was sent to ' + ','.join(self.receipts))
        except Exception as e:
            self.logger.error('failed at sending email: ' + str(e))
        finally:
            server.quit()

