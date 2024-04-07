from fasttraders.ultis.wrapper import safe_wrapper


class BaseStrategy:

    def __init__(self, bot):
        self.bot = bot

    """
    Base of strategies
    """

    def ft_bot_start(self, **kwargs) -> None:
        """
        Strategy init
        Must call bot_start()
        """

        safe_wrapper(self.bot_start)()

    def bot_start(self, **kwargs) -> None:
        """
        Called only once after bot instantiation.
        :param **kwargs: Ensure to keep this here so updates to this won't
        break your strategy.
        """
        pass
