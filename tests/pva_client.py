from p4p.client.thread import Context

if __name__ == "__main__":
    ctxt = Context("pva")
    print(ctxt.get("SIMPLE:DOUBLE"))
