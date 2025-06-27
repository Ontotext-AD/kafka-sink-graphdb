package com.ontotext.kafka.test.framework.tls;

public class TlsUtil {

	public static final String CERTIFICATE1 = "-----BEGIN CERTIFICATE-----\n" +
		"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\n" +
		"Q/h5zjHu2EEArJZ+jy6kjaVoZ4ricS4iEQ==\n" +
		"-----END CERTIFICATE-----";

	public static final String KEY1 = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
		"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\n" +
		"swIhNMW7MPxTt1OKp+d10Q==\n" +
		"-----END ENCRYPTED PRIVATE KEY-----";

	public static final String KEY1_PASS = "pass";

	public static final String CERTIFICATE2 = "-----BEGIN CERTIFICATE-----\n" +
		"MIIDmTCCAoGgAwIBAgIUesYCQkCJP1KBhR02TAYFc7812mwwDQYJKoZIhvcNAQEL\n" +
		"BQAwXDELMAkGA1UEBhMCQkcxDjAMBgNVBAgMBVNvZmlhMQ4wDAYDVQQHDAVTb2Zp\n" +
		"YTEhMB8GA1UECgwYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMQowCAYDVQQDDAFl\n" +
		"MB4XDTI1MDYyMzExNDU0M1oXDTI2MDYyMzExNDU0M1owXDELMAkGA1UEBhMCQkcx\n" +
		"DjAMBgNVBAgMBVNvZmlhMQ4wDAYDVQQHDAVTb2ZpYTEhMB8GA1UECgwYSW50ZXJu\n" +
		"ZXQgV2lkZ2l0cyBQdHkgTHRkMQowCAYDVQQDDAFlMIIBIjANBgkqhkiG9w0BAQEF\n" +
		"AAOCAQ8AMIIBCgKCAQEApqWEG53Wa/qn4pu/ho5dx/g3C+1ezLoLy3B0R3nEZRio\n" +
		"jegK1UlDJuKjsk1IElU/Yb7AcL+ZnhT2sxk1fgaf8fYOElfODPGE4s7gJVNRk2ga\n" +
		"361EMD3bC8WUsq2CtXOrinVhWW4Oxhplod2Uttq4MiGi8bd8HtDeFn3hFpXmYkDI\n" +
		"T8GNmtjS0y2634r5betELXKMq8cuK8NZ+r/zRSMPOc0EfESV8EFZywaJjUvcdFLh\n" +
		"NMxG4vDCrw0uIqad3LPEn/G3LL3T6zD7+8VoHK5bYWs0RsvllDL7+/BZYOPXlkOs\n" +
		"rtgQ2mX9eoGJZ8TvLhIk8I1mIjYOtYQidmvD/qSMiQIDAQABo1MwUTAdBgNVHQ4E\n" +
		"FgQUqsVrzrpe7hraSgqKrngxYIi8eV8wHwYDVR0jBBgwFoAUqsVrzrpe7hraSgqK\n" +
		"rngxYIi8eV8wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEATDxu\n" +
		"s2ogaEyjpBeSHboqvdXZZZDfhgzJjM45fnSl8TPYRTU/k2x4+130uJaSFUdRK+rg\n" +
		"qOpcUGR0CLJPxebYK8BdF/Un0NYSvQkSbMsCCUjWpfBqSuppqDXiHtrmfVtaPAtp\n" +
		"yHzUvURxDDzltA1XootSvFC49hfS8YSaVSGB+4Qx3tPv1n2zmSJzhuJcaYgmB3ki\n" +
		"HGUfR/SrdhR7e1TUxCsRJoTK4QWp8vKYuLFxqunYp9teN4iCNfVxU/KDdFXuFXgo\n" +
		"W46Rd+rESMDH+w2LiFAo+b5fscoAgfCcgnD8gAvxZLRMIfhffnYlgZ2ZaJ9qp+CZ\n" +
		"xwu8czz+TFeI4GIzog==\n" +
		"-----END CERTIFICATE-----";

	public static final String KEY2 = "-----BEGIN PRIVATE KEY-----\n" +
		"MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCmpYQbndZr+qfi\n" +
		"m7+Gjl3H+DcL7V7MugvLcHRHecRlGKiN6ArVSUMm4qOyTUgSVT9hvsBwv5meFPaz\n" +
		"GTV+Bp/x9g4SV84M8YTizuAlU1GTaBrfrUQwPdsLxZSyrYK1c6uKdWFZbg7GGmWh\n" +
		"3ZS22rgyIaLxt3we0N4WfeEWleZiQMhPwY2a2NLTLbrfivlt60Qtcoyrxy4rw1n6\n" +
		"v/NFIw85zQR8RJXwQVnLBomNS9x0UuE0zEbi8MKvDS4ipp3cs8Sf8bcsvdPrMPv7\n" +
		"xWgcrlthazRGy+WUMvv78Flg49eWQ6yu2BDaZf16gYlnxO8uEiTwjWYiNg61hCJ2\n" +
		"a8P+pIyJAgMBAAECggEAIS5JnJoTzJIvBV7PhsIoCB3zD+vmeUr9+Dbe4DBtm1eK\n" +
		"kB7IsWkR9tcfkuvyUrwoj5TEim6L7r7r1ANL9FjrVekRKjPTch5QwDxCwwvQ7VHI\n" +
		"MAs2zYgzaiegEOedY/WxDTOL+5t/U5CD9fPBnZr1G/44jHplO1mJLt9HhxC3u44p\n" +
		"tBG0QWXkOSKGYAAotforw+VZTbmwTSe62SyNrJYtYhBB995P9lZZXQZhL2Yf9o7o\n" +
		"8ZYm+3wIzHA+MhAlcIZdjZd7RkmMk2kCJ/7dphSbLJIHT8g+36gnApPvUBHL4YHA\n" +
		"tEQYg/+nSvpybezj061g13qkc9Yo5F3q9gCkbR/lkQKBgQDpmfxfYwRsjJS5HCaM\n" +
		"orb+7zuQ/rXZiQx2wJQo3gMuKAyT9qm2qahFMk6KIJaU5R/t61cnAxy+KkpBo9mq\n" +
		"z35YSn4zBPTvo1odjKOSMCzarzvbD02/qgXYONDiSJlkRaDv+ITz5ESKjs4XPxGE\n" +
		"4D5uVouZfk5SO7eoxUlxE7Jo/QKBgQC2oAtyd+Z+vUYrDLIUgirWz1ljygtOp7Jl\n" +
		"GkiI1eSIwyroYzpkc7vUmBiTJ75WCz+YJmDE5Djtz5XE/19kAD6pciHyGesCv60r\n" +
		"M1/MIqXFpq9DNWwCA9szSR4OCdl9KE66KVTYBUrWgr9xwmKDlLlG9M+Y6rcqn18i\n" +
		"w7hvaH89fQKBgHydoaz89HI3uGrbYpEpiDeNRZh+2GG4o4aCC0eVz9qCkNsp/bUs\n" +
		"4lAvmwhChDHf3N2d6vsrxNyJUN2dW9OsJvlQm+v5m/RujvcufF/TBUqPqn2ct1wk\n" +
		"hkOnY38jPXjpUAZT7BDzV8EWl0h9Y4of2g4gaw4x+/QQWVRWEmyZFla9AoGAboQz\n" +
		"6TW9+vy3td2c9uQ+4HfidI0UgqjhQGXDd2tZPZo+IRvLfna7OFBCruriOFfeSEkR\n" +
		"TpV+DgwNaoXQSEgNo6U5/bTJj4RHW1lfo71j7FSOw3FUx3Nl94dL9NrpDuCRo/7X\n" +
		"JVMuktnU2Rb1XwNljBciejuwVM7VV8hxmJn4RlECgYATlIdsXnhoTd5GgsPUKydD\n" +
		"gvsTLvNJM62+wyeEg5ZVTb02sFXPGbqt2rKtuT4lWoV7a/Ag32YWKLsI1vt8GLhF\n" +
		"bAUNh6PHai1a7QiQpIiUydZPKZV6F7jf5bQ2hdPKOC7ptJaB9/5PXvfaHFiiwsjq\n" +
		"+dmUOZvdq7ctoCYPreZW2Q==\n" +
		"-----END PRIVATE KEY-----";

}
