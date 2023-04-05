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
import os
from datetime import timezone

from tinydb import TinyDB
from web3 import Web3

from chief_keeper.spell import DSSSpell
from chief_keeper.spell import zero_address
from pymaker import Address
from pymaker.deployment import DssDeployment


class SimpleDatabase:
    """ Wraps around the logic to create, update, and query the Keeper's local database """

    def __init__(self, web3: Web3, network: str, deployment: DssDeployment):
        self.db = None
        self.web3 = web3
        self.network = network
        self.dss = deployment

    def create(self):
        """ Updates a locally stored database with the DS-Chief state since its last update.
        If a local database is not found, create one and query the DS-Chief state since its deployment.
        """
        parent_path = os.path.abspath(os.path.join("..", os.path.dirname(__file__)))
        filepath = os.path.abspath(os.path.join(parent_path, "database", "db_" + self.network + ".json"))

        if os.path.isfile(filepath) and os.access(filepath, os.R_OK):
            # checks if file exists
            result = "Simple database exists and is readable"
            self.db = TinyDB(filepath)
        else:
            result = "Either file is missing or is not readable, creating simple database"
            self.db = TinyDB(filepath)

            block_number = self.web3.eth.blockNumber
            self.db.insert({'last_block_checked': block_number})

            hat = self.dss.ds_chief.get_hat().address
            done = True
            eta = 0
            # if hat is zero address, then there is no active spell
            if hat != zero_address:
                spell = DSSSpell(self.web3, Address(hat))
                eta = get_eta_in_unix(spell)
                done = spell.done()
            print(f"init hat: {hat}, eta: {eta}, done: {done}")
            self.db.insert({'hat': {'address': hat, 'eta': eta, 'done': done}})
        return result

    def update_db_hat(self, current_block_number: int):
        """ Store yays that have been `etched` in DS-Chief since the last update """
        current_hat = self.dss.ds_chief.get_hat()
        old_hat = self.db.get(doc_id=2)["hat"]
        if old_hat.get('address') != current_hat.address:
            spell = DSSSpell(self.web3, current_hat)
            eta = get_eta_in_unix(spell)
            done = spell.done()
            self.db.update({'hat': {'address': current_hat.address, 'eta': eta, 'done': done}}, doc_ids=[2])
        self.db.update({'last_block_checked': current_block_number}, doc_ids=[1])

    def update_db_hat_eat(self, eta: int):
        """ Update the `eat` of the current hat """
        hat = self.db.get(doc_id=2)["hat"]
        hat["eta"] = eta
        self.db.update({'hat': hat}, doc_ids=[2])

    def update_db_hat_done(self, done: bool):
        """ Update the `done` of the current hat """
        hat = self.db.get(doc_id=2)["hat"]
        hat["done"] = done
        self.db.update({'hat': hat}, doc_ids=[2])


def get_eta_in_unix(spell: DSSSpell) -> int:
    eta = spell.eta()
    eta_in_unix = eta.replace(tzinfo=timezone.utc).timestamp()
    return int(eta_in_unix)
