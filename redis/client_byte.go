package redis

// Cmd calls the given Redis command.
func (c *Client) SendByte(data []byte) *Resp {
	return c.FSendByte(data).GetResp()
}

func (c *Client) FSendByte(data []byte) *Future {
	c.Lock()
	defer c.Unlock()

	f := NewFuture()
	if c.isClosing {
		f.resp <- newRespIOErr(ErrClientClosed)
		return f
	}
	err := c.writeByte(data)
	if err != nil {
		f.resp <- newRespIOErr(err)
	} else {
		//f.inTime = time.Now().UnixNano()
		c.futureChan <- f
	}
	return f
}

func (c *Client) writeByte(data []byte) error {
	//if c.timeout != 0 {
	//	c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	//}

	var err error
	c.send_size += len(data)
	c.writeChan <- data

	if err != nil {
		c.LastCritical = err
		c.closeWithError(err)
		return err
	}
	return nil
}
