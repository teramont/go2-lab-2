package main

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestBalancer(c *C) {
	c.Assert(chooseServer([]Server{}), Equals, -1)
	c.Assert(chooseServer([]Server{
		Server{Url: "", Alive: false, Connections: 0},
	}), Equals, -1)
	c.Assert(chooseServer([]Server{
		Server{Url: "", Alive: true, Connections: 0},
	}), Equals, 0)
	c.Assert(chooseServer([]Server{
		Server{Url: "", Alive: true, Connections: 20},
		Server{Url: "", Alive: true, Connections: 5},
		Server{Url: "", Alive: true, Connections: 10},
	}), Equals, 1)
	c.Assert(chooseServer([]Server{
		Server{Url: "", Alive: true, Connections: 20},
		Server{Url: "", Alive: false, Connections: 5},
		Server{Url: "", Alive: true, Connections: 10},
	}), Equals, 2)
}
