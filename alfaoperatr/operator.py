from asyncio import ensure_future, get_event_loop

from .controller import AlfaController

class AlfaOperator:
  def loop():
    get_event_loop().run_until_complete(ensure_future(AlfaController().loop()))
