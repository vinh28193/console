import sys
import threading
from collections import Counter
from .config import AppConfig


class Apps:
    """
    A registry that stores the configuration of installed applications.
    """

    def __init__(self, installed_apps=()):
        # installed_apps is set to None when creating the main registry
        # because it cannot be populated at that point. Other registries must
        # provide a list of installed apps and are populated immediately.
        if installed_apps is None and hasattr(sys.modules[__name__], "apps"):
            raise RuntimeError("You must supply an installed_apps argument.")

        # Mapping of labels to AppConfig instances for installed apps.
        self.app_configs = {}

        # Stack of app_configs. Used to store the current state in
        # set_available_apps and set_installed_apps.
        self.stored_app_configs = []

        # Whether the registry is populated.
        self.apps_ready = self.ready = False
        # For the autoreloader.
        self.ready_event = threading.Event()

        # Lock for thread-safe population.
        self._lock = threading.RLock()
        self.loading = False

        # Populate apps and models, unless it's the main registry.
        if installed_apps is not None:
            self.populate(installed_apps)

    def populate(self, installed_apps=None):
        """
        Load application configurations and models.

        Import each application module and then each model module.

        It is thread-safe and idempotent, but not reentrant.
        """
        if self.ready:
            return

        # populate() might be called by two threads in parallel on servers
        # that create threads before initializing the WSGI callable.
        with self._lock:
            if self.ready:
                return

            # An RLock prevents other threads from entering this section. The
            # compare and set operation below is atomic.
            if self.loading:
                # Prevent reentrant calls to avoid running AppConfig.ready()
                # methods twice.
                raise RuntimeError("populate() isn't reentrant")
            self.loading = True

            # Initialize app configs and import app modules.
            for entry in installed_apps:
                if isinstance(entry, AppConfig):
                    app_config = entry
                else:
                    app_config = AppConfig.create(entry)
                if app_config.label in self.app_configs:
                    raise ImportError(
                        "Application labels aren't unique, "
                        "duplicates: %s" % app_config.label
                    )

                self.app_configs[app_config.label] = app_config
                app_config.apps = self

            # Check for duplicate app names.
            counts = Counter(
                app_config.name for app_config in self.app_configs.values()
            )
            duplicates = [name for name, count in counts.most_common() if
                          count > 1]
            if duplicates:
                raise ImportError(
                    "Application names aren't unique, "
                    "duplicates: %s" % ", ".join(duplicates)
                )

            self.apps_ready = True

            self.clear_cache()

            self.models_ready = True

            # Phase 3: run ready() methods of app configs.
            for app_config in self.get_app_configs():
                app_config.ready()

            self.ready = True
            self.ready_event.set()

    def check_apps_ready(self):
        """Raise an exception if all apps haven't been imported yet."""
        if not self.apps_ready:
            from consoles.conf import settings

            # If "not ready" is due to unconfigured settings, accessing
            # INSTALLED_APPS raises a more helpful ImproperlyConfigured
            # exception.
            settings.INSTALLED_APPS
            raise Exception("Apps aren't loaded yet.")

    def get_app_configs(self):
        """Import applications and return an iterable of app configs."""
        self.check_apps_ready()
        return self.app_configs.values()

    def get_app_config(self, app_label):
        """
        Import applications and returns an app config for the given label.

        Raise LookupError if no application exists with this label.
        """
        self.check_apps_ready()
        try:
            return self.app_configs[app_label]
        except KeyError:
            message = "No installed app with label '%s'." % app_label
            for app_config in self.get_app_configs():
                if app_config.name == app_label:
                    message += " Did you mean '%s'?" % app_config.label
                    break
            raise LookupError(message)

    def is_installed(self, app_name):
        """
        Check whether an application with this name exists in the registry.
        """
        self.check_apps_ready()
        return any(ac.name == app_name for ac in self.app_configs.values())

    def get_containing_app_config(self, object_name):
        """
        Look for an app config containing a given object.

        object_name is the dotted Python path to the object.

        Return the app config for the inner application in case of nesting.
        Return None if the object isn't in any registered app config.
        """
        self.check_apps_ready()
        candidates = []
        for app_config in self.app_configs.values():
            if object_name.startswith(app_config.name):
                subpath = object_name[len(app_config.name):]
                if subpath == "" or subpath[0] == ".":
                    candidates.append(app_config)
        if candidates:
            return sorted(candidates, key=lambda ac: -len(ac.name))[0]

    def set_available_apps(self, available):
        """
        Restrict the set of installed apps used by get_app_config[s].

        available must be an iterable of application names.

        set_available_apps() must be balanced with unset_available_apps().

        Primarily used for performance optimization in TransactionTestCase.

        This method is safe in the sense that it doesn't trigger any imports.
        """
        available = set(available)
        installed = {app_config.name for app_config in self.get_app_configs()}
        if not available.issubset(installed):
            raise ValueError(
                "Available apps isn't a subset of installed apps, extra apps: "
                "%s"
                % ", ".join(available - installed)
            )

        self.stored_app_configs.append(self.app_configs)
        self.app_configs = {
            label: app_config
            for label, app_config in self.app_configs.items()
            if app_config.name in available
        }
        self.clear_cache()

    def unset_available_apps(self):
        """Cancel a previous call to set_available_apps()."""
        self.app_configs = self.stored_app_configs.pop()
        self.clear_cache()

    def set_installed_apps(self, installed):
        """
        Enable a different set of installed apps for get_app_config[s].

        installed must be an iterable in the same format as INSTALLED_APPS.

        set_installed_apps() must be balanced with unset_installed_apps(),
        even if it exits with an exception.

        Primarily used as a receiver of the setting_changed signal in tests.

        This method may trigger new imports, which may add new models to the
        registry of all imported models. They will stay in the registry even
        after unset_installed_apps(). Since it isn't possible to replay
        imports safely (e.g. that could lead to registering listeners twice),
        models are registered when they're imported and never removed.
        """
        if not self.ready:
            raise Exception("App registry isn't ready yet.")
        self.stored_app_configs.append(self.app_configs)
        self.app_configs = {}
        self.apps_ready = self.loading = self.ready = False
        self.clear_cache()
        self.populate(installed)

    def unset_installed_apps(self):
        """Cancel a previous call to set_installed_apps()."""
        self.app_configs = self.stored_app_configs.pop()
        self.apps_ready = self.ready = True
        self.clear_cache()

    def clear_cache(self):
        """
        Clear all internal caches, for methods that alter the app registry.

        This is mostly used in tests.
        """


apps = Apps(installed_apps=None)
