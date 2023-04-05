# This file is part of the Maker Keeper Framework.
#
# Copyright (C) 2020 KentonPrescott
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import logging
import sys

from web3 import Web3
from web3.middleware import geth_poa_middleware

from auction_keeper.gas import DynamicGasPrice
from chief_keeper.database import SimpleDatabase, get_eta_in_unix
from chief_keeper.spell import DSSSpell, zero_address
from pymaker import Address, web3_via_http
from pymaker.deployment import DssDeployment
from pymaker.keys import register_keys
from pymaker.lifecycle import Lifecycle
from pymaker.util import is_contract_at


class ChiefKeeper:
    """Keeper that lifts the hat and streamlines executive actions"""

    logger = logging.getLogger('chief-keeper')

    def __init__(self, args: list, **kwargs):
        """Pass in arguements assign necessary variables/objects and instantiate other Classes"""

        parser = argparse.ArgumentParser("chief-keeper")

        parser.add_argument("--rpc-host", type=str, default="https://localhost:8545",
                            help="JSON-RPC host:port (default: 'localhost:8545')")

        parser.add_argument("--rpc-timeout", type=int, default=15,
                            help="JSON-RPC timeout (in seconds, default: 15)")

        parser.add_argument("--network", type=str, required=True,
                            help="Network that you're running the Keeper on (options, 'mainnet', 'kovan', 'testnet')")

        parser.add_argument("--eth-from", type=str, required=True,
                            help="Ethereum address from which to send transactions; checksummed (e.g. '0x12AebC')")

        parser.add_argument("--eth-key", type=str, nargs='*',
                            help="Ethereum private key(s) to use (e.g. 'key_file=/path/to/keystore.json,pass_file=/path/to/passphrase.txt')")

        parser.add_argument("--dss-deployment-file", type=str, required=False,
                            help="Json description of all the system addresses (e.g. /Full/Path/To/configFile.json)")

        parser.add_argument("--max-errors", type=int, default=100,
                            help="Maximum number of allowed errors before the keeper terminates (default: 100)")

        parser.add_argument("--debug", dest='debug', action='store_true',
                            help="Enable debug output")

        parser.add_argument("--ethgasstation-api-key", type=str, default=None, help="ethgasstation API key")
        parser.add_argument("--etherchain_gas", type=str, default=None, help="etherscan API key")
        parser.add_argument("--poanetwork_gas", type=str, default=None, help="poanetwork API key")
        parser.add_argument("--fixed_gas_price", type=float, default=None, help="fixed gas price")
        parser.add_argument("--gas-initial-multiplier", type=str, default=1.1,
                            help="initial gas prices = gas-initial-multiplier * node-gas-prices")
        parser.add_argument("--gas-reactive-multiplier", type=str, default=1.25,
                            help="if tx can't be sent, evenry 42 seconds gas price is multiplied by this value")
        parser.add_argument("--gas-maximum", type=str, default=5000, help="maximum gas price")

        parser.set_defaults(cageFacilitated=False)
        self.arguments = parser.parse_args(args)

        self.web3: Web3 = kwargs['web3'] if 'web3' in kwargs else web3_via_http(
            endpoint_uri=self.arguments.rpc_host, timeout=self.arguments.rpc_timeout, http_pool_size=100)

        # fix poa chain
        self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.web3.eth.defaultAccount = self.arguments.eth_from
        register_keys(self.web3, self.arguments.eth_key)
        self.our_address = Address(self.arguments.eth_from)

        if self.arguments.dss_deployment_file:
            self.dss = DssDeployment.from_json(web3=self.web3,
                                               conf=open(self.arguments.dss_deployment_file, "r").read())
        else:
            self.dss = DssDeployment.from_network(web3=self.web3, network=self.arguments.network)

        self.max_errors = self.arguments.max_errors
        self.errors = 0

        self.confirmations = 0
        self.gas_price = DynamicGasPrice(self.arguments, self.web3)
        logging.basicConfig(format='%(asctime)-15s %(levelname)-8s %(message)s',
                            level=(logging.DEBUG if self.arguments.debug else logging.INFO))

    def main(self):
        """ Initialize the lifecycle and enter into the Keeper Lifecycle controller.

        Each function supplied by the lifecycle will accept a callback function that will be executed.
        The lifecycle.on_block() function will enter into an infinite loop, but will gracefully shutdown
        if it recieves a SIGINT/SIGTERM signal.
        """

        with Lifecycle(self.web3) as lifecycle:
            self.lifecycle = lifecycle
            self.lifecycle.do_wait_for_sync = False
            lifecycle.on_startup(self.check_deployment)
            lifecycle.on_block(self.process_block)

    def check_deployment(self):
        self.logger.info('')
        self.logger.info('Please confirm the deployment details')
        self.logger.info(f'Keeper Balance: {self.web3.eth.getBalance(self.our_address.address) / (10 ** 18)} ETH')
        self.logger.info(f'DS-Chief: {self.dss.ds_chief.address}')
        self.logger.info(f'DS-Pause: {self.dss.pause.address}')
        self.logger.info('')
        self.initial_query()

    def initial_query(self):
        """ Updates a locally stored database with the DS-Chief state since its last update.
        If a local database is not found, create one and query the DS-Chief state since its deployment.
        """
        self.logger.info('')
        self.logger.info('Querying DS-Chief state since last update ( !! Could take up to 15 minutes !! )')

        self.database = SimpleDatabase(self.web3,
                                       self.arguments.network,
                                       self.dss)
        result = self.database.create()

        self.logger.info(result)

    def process_block(self):
        """ Callback called on each new block. If too many errors, terminate the keeper.
        This is the entrypoint to the Keeper's monitoring logic
        """
        if self.errors >= self.max_errors:
            self.lifecycle.terminate()
        else:
            self.check_hat()
            self.check_eta()

    def check_hat(self):
        """ Ensures the Hat is on the proposal (spell, EOA, multisig, etc) with the most approval.

        First, the local database is updated with proposal addresses (yays) that have been `etched` in DSChief between
        the last block reviewed and the most recent block receieved. Next, it simply traverses through each address,
        checking if its approval has surpased the current Hat. If it has, it will `lift` the hat.

        If the current or new hat hasn't been casted nor plotted in the pause, it will `schedule` the spell
        """
        block_number = self.web3.eth.blockNumber
        self.logger.info(f'Checking Hat on block {block_number}')
        self.database.update_db_hat(block_number)
        hat = self.database.db.get(doc_id=2)["hat"]
        # check hat is not 0x0 and not done
        hat_address = hat.get("address", zero_address)
        if hat_address == zero_address or hat.get("done"):
            return

        # check hat is contract
        if not is_contract_at(self.web3, Address(hat_address)):
            self.logger.info(f'Current hat ({hat}) is not a contract')
            return
        spell = DSSSpell(self.web3, Address(hat_address))
        # if hat not done, schedule
        if hat.get("done") is False and hat.get("eta", 0) == 0:
            spell_eta = get_eta_in_unix(spell)
            if spell_eta > 0:
                self.logger.info(f'spell already scheduled, hat ({hat}), eta: {spell_eta}')
                self.database.update_db_hat_eat(spell_eta)
                return
            self.logger.info(f'Scheduling hat ({hat})')
            spell.schedule().transact(gas_price=self.gas_price)
            spell_eta = get_eta_in_unix(spell)
            if spell_eta > 0:
                self.database.update_db_hat_eat(spell_eta)
            return

    def check_eta(self):
        """ Cast spells that meet their schedule.

        First, the local database is updated with spells that have been scheduled between the last block
        reviewed and the most recent block receieved. Next, it simply traverses through each spell address,
        checking if its schedule has been reached/passed. If it has, it attempts to `cast` the spell.
        """

        hat = self.database.db.get(doc_id=2)["hat"]
        hat_address = hat.get("address", zero_address)
        hat_eta = hat.get("eta", 0)
        if hat_address == zero_address or hat.get("done") or hat_eta == 0:
            return

        block_number = self.web3.eth.blockNumber
        block = self.web3.eth.getBlock(block_number)
        if block is None:
            self.logger.warning(f'Checking eta get is None block {block_number}')
            return
        now = block.timestamp
        self.logger.info(f'Checking scheduled spells on block {block_number}')

        if now < hat_eta:
            return

        spell = DSSSpell(self.web3, Address(hat_address)) if is_contract_at(self.web3, Address(hat_address)) else None
        self.logger.info(f'Casting spell ({spell.address.address})')
        receipt = spell.cast().transact(gas_price=self.gas_price)

        if receipt is None or receipt.successful == True:
            self.database.update_db_hat_done(True)


if __name__ == '__main__':
    ChiefKeeper(sys.argv[1:]).main()
