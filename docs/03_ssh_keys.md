# 02 – SSH Keys: Identity and Access

This document explains what SSH keys are, why they are required, and how they are used for machine authentication.

---

## Why SSH Keys Are Required

When a machine (laptop or server) connects to services such as GitHub or a runtime host, it must prove its identity.

Passwords are insecure and are not used for system-level authentication.

SSH keys provide cryptographic identity.  
Each machine is treated as a unique identity.

Mental model:

- Machine = identity  
- SSH key = cryptographic credential  

GitHub does not trust users.  
GitHub trusts machines that can prove possession of a private key.

---

## What an SSH Key Is

An SSH key consists of a key pair:

Private key (never shared):  
~/.ssh/id_ed25519  

Public key (safe to share):  
~/.ssh/id_ed25519.pub  

The public key is registered with external services.  
The private key remains only on the originating machine.

---

## How Authentication Works

1. A machine connects to a service  
2. The service checks if the machine’s public key is registered  
3. The service sends a cryptographic challenge  
4. The machine signs the challenge with its private key  
5. If the signature matches, access is granted  

At no point are passwords transmitted.

---

## Key Generation

Run on the machine that requires access:

    ssh-keygen -t ed25519 -C "machine-name"

Accept default file location.  
Passphrase is optional.

---

## Key Locations

Private key:  
~/.ssh/id_ed25519  

Public key:  
~/.ssh/id_ed25519.pub  

---

## Registering with GitHub

Display the public key:

    cat ~/.ssh/id_ed25519.pub

Copy the entire output line.

Register at:

https://github.com/settings/keys

Create a new SSH key and paste the value.

---

## Verification

Test authentication:

    ssh -T git@github.com

Expected response:

Hi <username>! You've successfully authenticated...

---

## Operational Rule

Every machine must use its own SSH key.

Laptop key ≠ Server key

If a server connects to GitHub or other services:

- It must generate its own key  
- Its public key must be registered separately  

Keys are never shared across machines.

---

## Runtime Host Access

The runtime host is accessed via SSH:

    ssh root@<RUNTIME_IP>

Authentication uses the private key on the local machine.  
No passwords are used.

If access fails, the cause is always one of:

- wrong IP  
- wrong key  
- firewall blocking the connection  

Never debug application issues before SSH works.

---

## Security Model

Public key = lock  
Private key = key  

The service holds the lock.  
The machine proves possession of the key.

Identity is bound to machines, not people.
