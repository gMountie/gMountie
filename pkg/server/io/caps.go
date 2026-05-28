package io

import (
	"golang.org/x/sys/unix"
)

// String identifiers stored in service.Identity.Caps / io.Identity.Caps.
// Must match the strings used in volume config (caps: [...]) and in the
// system resolver's admin_groups keys.
const (
	CapDacReadSearch = "dac_read_search"
	CapDacOverride   = "dac_override"
)

// Linux cap bit numbers (per <linux/capability.h>).
const (
	capChown         = 0
	capDacOverride   = 1
	capDacReadSearch = 2
	capFowner        = 3
	capFsetid        = 4
	capSetgid        = 6
	capSetuid        = 7
)

// capState is the EFFECTIVE/PERMITTED/INHERITABLE 64-bit triple in flat form.
// We mutate EFFECTIVE only — PERMITTED is monotonically non-increasing on
// Linux, so dropping a cap from PERMITTED forfeits the ability to re-raise it.
type capState struct {
	effective, permitted, inheritable uint64
}

// getCaps reads the current thread's capability sets.
// unix.Capget/Capset target the calling thread (Pid=0) and do NOT broadcast
// to all threads (unlike Go's syscall.Setgroups), so they are safe to call on
// a LockOSThread-pinned goroutine.
func getCaps() (capState, error) {
	hdr := &unix.CapUserHeader{
		Version: unix.LINUX_CAPABILITY_VERSION_3,
		Pid:     0,
	}
	var data [2]unix.CapUserData
	if err := unix.Capget(hdr, &data[0]); err != nil {
		return capState{}, err
	}
	return capState{
		effective:   uint64(data[0].Effective) | (uint64(data[1].Effective) << 32),
		permitted:   uint64(data[0].Permitted) | (uint64(data[1].Permitted) << 32),
		inheritable: uint64(data[0].Inheritable) | (uint64(data[1].Inheritable) << 32),
	}, nil
}

// setCapsEffective writes back a new capability state. Only EFFECTIVE is
// expected to change between calls; PERMITTED and INHERITABLE are passed
// through unmodified to avoid violating the monotonicity constraint.
func setCapsEffective(c capState) error {
	hdr := &unix.CapUserHeader{
		Version: unix.LINUX_CAPABILITY_VERSION_3,
		Pid:     0,
	}
	var data [2]unix.CapUserData
	data[0].Effective = uint32(c.effective)
	data[1].Effective = uint32(c.effective >> 32)
	data[0].Permitted = uint32(c.permitted)
	data[1].Permitted = uint32(c.permitted >> 32)
	data[0].Inheritable = uint32(c.inheritable)
	data[1].Inheritable = uint32(c.inheritable >> 32)
	return unix.Capset(hdr, &data[0])
}

// dacReadSearchEffectiveMask returns the bit mask the dac_read_search path
// keeps in EFFECTIVE. Drops DAC_OVERRIDE/FOWNER/FSETID; keeps DAC_READ_SEARCH
// + SETUID/SETGID (the latter two so the bound-FS restore path can put fsuid
// and fsgid back via setfsuid/setfsgid).
func dacReadSearchEffectiveMask() uint64 {
	return (1 << capDacReadSearch) | (1 << capSetuid) | (1 << capSetgid)
}
