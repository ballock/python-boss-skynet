#!/usr/bin/python
# -*- coding: utf-8; tab-width: 4 -*-
import os, sys, glob, traceback, signal
import ConfigParser
from  RuoteAMQP.participant import Participant
from SkyNET.Control import WorkItemCtrl
import types
import logging

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s', level=logging.INFO)

DEFAULT_SKYNET_CONFIG_DIR = "/etc/skynet/"
DEFAULT_SKYNET_CONFIG_FILE = "/etc/skynet/skynet.conf"

class ParticipantHandlerNotDefined(RuntimeError):
    def __init__(self):
        super(ParticipantHandlerNotDefined, self).__init__(
                "ParticipantHandler class not found")

class InvalidParticipantHandlerSignature(RuntimeError):
    pass

class ParticipantConfigError(RuntimeError):
    def __init__(self, opt, section, reason="missing"):
        super(ParticipantConfigError, self).__init__(
            "Option '%s' for section '%s' is %s" %
            (opt, section, reason))

def workitem_summary(wid):
    parts = ["Taking workitem"]
    if wid.fields.ev and wid.fields.ev.id:
        parts.append("#%s" % wid.fields.ev.id)
    if wid.fields.project:
        parts.append("for %s" % wid.fields.project)
    # Put a colon after whatever we got so far
    parts = [' '.join(parts) + ":"]
    if wid.participant_name:
        parts.append(wid.participant_name)
    if wid.params:
        for key, value in wid.params.as_dict().items():
            # Remove some uninteresting parameters from the log
            if key in ['participant_options', 'if']:
                continue
            if key == 'ref' and wid.participant_name:
                continue
            parts.append("%s=%s" % (key, repr(value)))
    return ' '.join(parts)

class ExoParticipant(Participant):
    """
    This class runs the normal participant handling code.
    In order to support some sophisticated Ruote usage it writes a closure
    into the ParticipantHandler namespace called send_to_engine()
    This closure invokes *this* objects send_to_engine() method and
    uses that to call the super write_to_engine()
    """
    def __init__(self, exo=None, *args, **kwargs):
        super(ExoParticipant, self).__init__(*args, **kwargs)
        self.exo = exo
        # Write a closure into the ParticipantHandler namespace
        self.exo.handler.send_to_engine = types.MethodType(
                lambda orig_obj, wi : self.send_to_engine(wi),
                self.exo.handler, self.exo.handler.__class__)

    def consume(self):
        """Workitem consumer.

        This method calls the ParticipantHandler.handle_wi() method.

        It also handles the following common tasks:

          * If workitem.fields.debug_dump or workitem.params.debug_dump is
            defined, workitem is dumped to participant log

        """
        if self.workitem.fields.debug_trace:
            self.exo.handler.log.info(workitem_summary(self.workitem))
        if self.workitem.fields.debug_dump or self.workitem.params.debug_dump:
            self.exo.handler.log.info(self.workitem.dump())
        self.exo.handler.handle_wi(self.workitem)

    def send_to_engine(self, witem):
        self.reply_to_engine(workitem=witem)

class Exo(object):
    """
    The Exo class provides the SkyNET participant exoskeleton.  This
    allows a consistent control framework to be applied to all BOSS
    participants. It requires a ParticipantHandler class to be provided in a
    dynamically imported python file. This class must provide the
    following interface:

    ParticipantHandler.handle_wi_control(WorkItemCtrl)
      This method is called with messages relating to the work being
      done and possibly from the process engine Typically this
      includes "cancel" and "status" messages.

    ParticipantHandler.handle_lifecycle_control(ParticipantCtrl)
      This method is called by SkyNET with messages relating to the
      Participant itself. Typically this includes "shutdown" type
      messages.

    ParticipantHandler.handle_wi(Workitem)

      This replaces the old consume() method and passes in a Workitem;
      code like:

        def consume(self):
          wi = self.workitem
          ...

      is replaced by:

        def handle_wi(wi):
          ...

    The messages objects passed are obtained using:
      from SkyNET import (WorkItemCtrl, ParticipantCtrl, Workitem)

    In SkyNET, Exo provides:

    Exo.WorkT : This thread is where work takes place and calls

    Exo.ControlT : This thread is where SkyNET control messages are handled

    Exo.BOSST : This thread is where BOSS control messages are handled

    Logging is done via stdout which is connected to a reliable
    logging service.

    """

    def __init__(self, local_config_file=None):
        """
        An Exo object is initialised with a config file providing values
        for this participant.

        Config information is read from the default location :
          /etc/skynet/skynet.conf
          which can be overridden by the 'local_config_file'
        """
        self.log = logging.getLogger("Exo")

        self.queue = self.amqp_host = self.amqp_user = self.amqp_pwd = \
                self.amqp_vhost = self.code = self.codepath = self.name = \
                self.config = self.graceful_shutdown = None

        self.parse_config(local_config_file)

        # Dynamically load the user code.
        # We are running as a normal user anyway at this point
        # Don't catch any errors here.
        sys.path.insert(0, self.codepath)
        p_namespace = __import__(self.code)

        # Create an I woinstance
        for key in ("name", "amqp_host", "amqp_user", "amqp_pwd", "amqp_vhost"):
            self.log.debug("%s : %s" % (key, getattr(self, key, "???")))

        # Complain if there is no ParticipantHandler class
        try:
            self.handler = p_namespace.ParticipantHandler()
        except NameError, exobj:
            raise ParticipantHandlerNotDefined()
        except TypeError, exobj:
            raise InvalidParticipantHandlerSignature(str(exobj))
        except Exception, exobj:
            raise exobj

        self.handler.log = self.log
        # An ExoParticipant knows about the handler
        self.p = ExoParticipant(exo=self,
                                ruote_queue=self.queue,
                                amqp_host=self.amqp_host,
                                amqp_user=self.amqp_user,
                                amqp_pass=self.amqp_pwd,
                                amqp_vhost=self.amqp_vhost)

        # FIXME : check for the mandatory methods

    def parse_config(self, config_file):
        config = ConfigParser.SafeConfigParser()
        config.read([DEFAULT_SKYNET_CONFIG_FILE, config_file])
        if config.has_option("skynet", "include_dir"):
            if os.path.exists(config.get("skynet", "include_dir")):
                for filename in glob.glob("%s/*.conf" % config.get("skynet",
                                                            "include_dir")):
                    try:
                        config.read(filename)
                    except ConfigParser.ParsingError, why:
                        self.log.exception(ValueError(str(why)))

        self.config = config

        # Validate the BOSS section options
        section = "boss"
        for opt in ("amqp_vhost", "amqp_pwd", "amqp_user", "amqp_host"):
            if not config.has_option(section, opt):
                raise ParticipantConfigError(opt, section)
            else:
                setattr(self, opt, config.get(section, opt))

        # Get participant config
        section = "participant"
        for opt in ("name", "queue", "code"):
            if config.has_option(section, opt):
                setattr(self, opt, config.get(section, opt))
        # Make sure we have name
        if not self.name:
            raise ParticipantConfigError("name", section)
        self.log.name = self.name

        # If there's no queue, use participant name
        if not self.queue:
            self.queue = self.name

        # if code is not given expect participant_logic in cwd
        if not self.code:
            self.code = "participant_logic.py"

        # convert the code to path and module name
        self.codepath, self.code = os.path.split(os.path.abspath(self.code))
        self.code, _, ext = self.code.partition(".")
        if not ext == "py":
            raise ParticipantConfigError("code", section,
                "invalid: Not a .py file")

        # Finally read "/etc/skynet/<pname>.conf", not caring if it exists
        config.read([DEFAULT_SKYNET_CONFIG_DIR + self.name + ".conf"])

        # Allow any of the configs to create a [skynet] section and
        # override the log_level
        if config.has_option("skynet", "log_level"):
            try:
                self.log.setLevel(config.get("skynet", "log_level"))
            except ValueError, why:
                self.log.exception(ValueError(str(why)))


    # Signals and threads are tricky.
    # Ensure that only the main thread sets the handler

    # FIXME: We should see if Pika can catch Interrupted system call

    def sighandler(self, signum, frame):
        self.log.debug("Caught signal %s", signum)
        if signum == signal.SIGTERM:
            self.p.finish()
            self.graceful_shutdown = True
        elif signum == signal.SIGSTOP:
            pass
        elif signum == signal.SIGHUP:
            pass
        elif signum == signal.SIGALRM:
            pass
        elif signum == signal.SIGINT:
            pass
        else:
            pass

    def run(self):
        # Enter event loop with some trial at clean exit

        self.graceful_shutdown = False
        # while self.p.running
        while True:
            try:
                # Install a handler
                signal.signal(signal.SIGTERM, self.sighandler)
                # Now ensure that system calls are restarted.  The
                # handler won't be called until the system call
                # returns
                #
                # mmm - actually this causes python to not exit the
                # run() loop if the system is quiet. We should allow
                # run to be re-startable and escalate from graceful to
                # hard.
                #
                # signal.siginterrupt(signal.SIGTERM, False)

                self.log.info("Now starting")
                msg = WorkItemCtrl("start")
                msg.config = self.config
                self.handler.handle_lifecycle_control(msg)
                self.p.run()
                if self.graceful_shutdown:
                    break

            except KeyboardInterrupt:
                logging.shutdown()
                sys.exit(0)

            except IOError as e:
                self.log.warning("p.run() interrupted - IOError %s" % e)
                if self.graceful_shutdown:
                    self.log.info("Now shutting down")
                    self.handler.handle_lifecycle_control(WorkItemCtrl("die"))
                    logging.shutdown()
                    sys.exit(1)

                self.log.info("Trying to shutdown gracefully")
                self.handler.handle_lifecycle_control(WorkItemCtrl("stop"))
                self.graceful_shutdown = True

            except Exception as e:
                self.log.warning("p.run() interrupted - %s" % e)
                traceback.print_exc()
                logging.shutdown()
                sys.exit(1)
