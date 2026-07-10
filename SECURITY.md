# Security policy

Report suspected vulnerabilities privately through GitHub: Security tab,
"Report a vulnerability" (private advisory). If that is not an option, email
joshrotenberg@gmail.com with the details.

Please do not open public issues for security reports. Include the module
version or commit, the server version, and a reproduction if you have one.

Scope worth knowing: this module runs inside the Redis server process and
writes only to keys under its configured `stream-prefix`. Reports about
untrusted event names, key contents reaching logs or streams, or behavior
under hostile co-loaded modules are all in scope; two hardening cases of that
kind are already documented in SPEC.md section 17 and the upstream issues it
links.
