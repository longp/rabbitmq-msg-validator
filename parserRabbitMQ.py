#!/usr/bin/python
# vim : set fileencoding=utf-8 :
#
# parserRabbitMQ.py
#
# Receive message from a rabbitMQ queue, validate it and send result on another rabbitMQ queue
#
#

import os.path

# configparser => python 3
try:
    import ConfigParser
#    assert ConfigParser
except ImportError:
    import configparser as ConfigParser

from optparse import OptionParser
import signal
import os
import sys
import pika
import json

class ParserRabbitMQMsg:
    def __init__(self, filename):
        # Use config parser to init
        config = ConfigParser.ConfigParser()
        config.read(filename)
        try:
            self.input_rabbit_host = config.get('input', 'rabbit_host')
            self.input_rabbit_port =int( config.get('input', 'rabbit_port'))
            self.input_queue = config.get('input', 'queue')
            self.input_rabbit_user = config.get('input', 'rabbit_user')
            self.input_rabbit_password = config.get('input', 'rabbit_password')

            self.output_rabbit_host = config.get('output', 'rabbit_host')
            self.output_rabbit_port = int(config.get('output', 'rabbit_port'))
            self.output_queue = config.get('output', 'queue')
            self.output_rabbit_user = config.get('output', 'rabbit_user')
            self.output_rabbit_password = config.get('output', 'rabbit_password')

            self.stdin = '/dev/null'
            self.stdout = config.get('daemon', 'log_file')
            self.stderr = config.get('daemon', 'err_file')
            self.pidfile = config.get('daemon', 'pid_file')
        except ConfigParser.NoOptionError, e:
            print "Option missing :", str(e)
            sys.exit(2)
        except ConfigParser.NoSectionError, e:
            print "Section missing :", str(e)
            sys.exit(2)

    # Signal handler. Only set a boolean to stop the loop
    def manage_signal(self, sig, frame):
        print "[parserRabbitMQ] I received a signal %d. Shutting down" % sig
        self.interrupted = True

    # Set signal handlers. Will call manage_signal for the list of signal above
    def set_sig_handlers(self):
        func = self.manage_signal
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGUSR1, signal.SIGUSR2, signal.SIGHUP):
            signal.signal(sig, func)

    # Main daemon loop. Check for file modification before parsing again the alert file
    def do_mainloop(self):
        # Create connection for input and output queue
        inputId = pika.credentials.PlainCredentials(
            self.input_rabbit_user,
            self.input_rabbit_password
        )

        outputId = pika.credentials.PlainCredentials(
            self.output_rabbit_user,
            self.output_rabbit_password
        )

        inputConnection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.input_rabbit_host,
                port=self.input_rabbit_port,
                credentials=inputId
            )
        )

        outputConnection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.output_rabbit_host,
                port=self.output_rabbit_port,
                credentials=outputId
            )
        )

        inputChannel = inputConnection.channel()
        outputChannel = outputConnection.channel()

        # Callback parse our message from the input queue, validate it
        # and send a message to the output queue
        def callback(ch, method, properties, body):
            msg = json.loads(body)
            outputChannel.basic_publish(exchange='',
                                        routing_key=self.output_queue,
                                        body=msg['firstName'])


        inputChannel.basic_consume(callback, queue=self.input_queue, no_ack=True)
        try:
            inputChannel.start_consuming()
        except Exception, e:
            sys.stderr.write(e.strerror)
            inputChannel.stop_consuming()
            inputConnection.close()
            outputConnection.close()

    # Do start the program, simply open file and run if not a daemon
    def do_start(self, is_daemon):
        if is_daemon:
            self.daemonize()
        self.set_sig_handlers()
        self.do_mainloop()

    # Stop the program and close file
    def do_stop(self, is_daemon):
        if is_daemon:
            os.remove(self.pidfile)
            sys.stdin.close()
            sys.stdout.close()
            sys.stderr.close()

    # Daemonize part, adapted from
    # http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("[parserRabbitMQ] Fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(5)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0133)  # The process will only create log file rw-r--r-- is fine

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("[parserRabbitMQ] Fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(5)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, 'r')
        so = open(self.stdout, 'a+')
        se = open(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        pid = str(os.getpid())
        open(self.pidfile, 'w+').write("%s\n" % pid)


# Main function. Parses options and creates the rabbitMQ parser
def main():
    parser = OptionParser()
    parser.add_option("-d", "--daemon", dest="is_daemon", action="store_true", default=False,
                      help="run as a daemon")
    parser.add_option("-c", "--config-file", dest="filename", default="/etc/daemonRabbitMQ.cfg",
                      help="daemon configuration file", metavar="FILE")

    (options, args) = parser.parse_args()

    if os.getuid() != 0:
        print "This must be run as root"
        sys.exit(3)

    if not options.filename:
        print "Missing configuration file!"
        sys.exit(4)

    rabbitmq_parser = ParserRabbitMQMsg(options.filename)
    rabbitmq_parser.do_start(options.is_daemon)
    rabbitmq_parser.do_stop(options.is_daemon)


if __name__ == "__main__":
    main()
