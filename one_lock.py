from  lock import Locks

lock_one = Locks("server 1")

lock_one.acquire_or_wait("server 1","/locks")
