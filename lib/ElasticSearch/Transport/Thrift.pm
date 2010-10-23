package ElasticSearch::Transport::Thrift;

use strict;
use warnings FATAL => 'all';
use ElasticSearch::Transport::ThriftBackend::Rest;
use ElasticSearch::Transport::ThriftBackend::Thrift;
use ElasticSearch::Transport::ThriftBackend::Thrift::Socket;
use ElasticSearch::Transport::ThriftBackend::Thrift::FramedTransport;
use ElasticSearch::Transport::ThriftBackend::Thrift::BinaryProtocol;

use Encode qw(decode_utf8);

use parent 'ElasticSearch::Transport';

my %Methods = (
    'GET'     => ElasticSearch::Transport::ThriftBackend::Method::GET,
    'PUT'     => ElasticSearch::Transport::ThriftBackend::Method::PUT,
    'POST'    => ElasticSearch::Transport::ThriftBackend::Method::POST,
    'DELETE'  => ElasticSearch::Transport::ThriftBackend::Method::DELETE,
    'HEAD'    => ElasticSearch::Transport::ThriftBackend::Method::HEAD,
    'OPTIONS' => ElasticSearch::Transport::ThriftBackend::Method::OPTIONS
);

my %Statuses = (
    100 => 'CONT',
    101 => 'SWITCHING_PROTOCOLS',
    200 => 'OK',
    201 => 'CREATED',
    202 => 'ACCEPTED',
    203 => 'NON_AUTHORITATIVE_INFORMATION',
    204 => 'NO_CONTENT',
    205 => 'RESET_CONTENT',
    206 => 'PARTIAL_CONTENT',
    207 => 'MULTI_STATUS',
    300 => 'MULTIPLE_CHOICES',
    301 => 'MOVED_PERMANENTLY',
    302 => 'FOUND',
    303 => 'SEE_OTHER',
    304 => 'NOT_MODIFIED',
    305 => 'USE_PROXY',
    307 => 'TEMPORARY_REDIRECT',
    400 => 'BAD_REQUEST',
    401 => 'UNAUTHORIZED',
    402 => 'PAYMENT_REQUIRED',
    403 => 'FORBIDDEN',
    404 => 'NOT_FOUND',
    405 => 'METHOD_NOT_ALLOWED',
    406 => 'NOT_ACCEPTABLE',
    407 => 'PROXY_AUTHENTICATION',
    408 => 'REQUEST_TIMEOUT',
    409 => 'CONFLICT',
    410 => 'GONE',
    411 => 'LENGTH_REQUIRED',
    412 => 'PRECONDITION_FAILED',
    413 => 'REQUEST_ENTITY_TOO_LARGE',
    414 => 'REQUEST_URI_TOO_LONG',
    415 => 'UNSUPPORTED_MEDIA_TYPE',
    416 => 'REQUESTED_RANGE_NOT_SATISFIED',
    417 => 'EXPECTATION_FAILED',
    422 => 'UNPROCESSABLE_ENTITY',
    423 => 'LOCKED',
    424 => 'FAILED_DEPENDENCY',
    500 => 'INTERNAL_SERVER_ERROR',
    501 => 'NOT_IMPLEMENTED',
    502 => 'BAD_GATEWAY',
    503 => 'SERVICE_UNAVAILABLE',
    504 => 'GATEWAY_TIMEOUT',
    506 => 'INSUFFICIENT_STORAGE',
);

#===================================
sub protocol {'thrift'}
#===================================

#===================================
sub send_request {
#===================================
    my $self   = shift;
    my $server = shift;
    my $params = shift;

    my $method = $params->{method};
    $self->throw( 'Param', "Unknown thrift method '$method'" )
        unless exists $Methods{$method};

    my $request = ElasticSearch::Transport::ThriftBackend::RestRequest->new();
    $request->method( $Methods{$method} );
    $request->uri( $params->{cmd} );
    $request->parameters( $params->{qs} || {} );
    $request->body( encode_utf8( $params->{data} || '{}' ) );
    $request->headers( {} );

    my $response;
    eval {
        my $client = $self->client($server);
        $response = $client->execute($request);
        }
        or do {
        my $error = $@ || 'Unknown';
        if ( ref $error && $error->{message} ) {
            $self->throw( 'Timeout', $error->{message} )
                if $error->{message} =~ /TSocket: timed out/;
            $self->throw( 'Connection', $error->{message} );
        }
        $self->throw( 'Request', $error );
        };

    my $content = $response->body;
    $content = decode_utf8($content) if defined $content;

    my $code = $response->status;
    my $msg  = $Statuses{$code};

    return $content if $msg eq 'OK';

    my $type
        = $msg eq 'REQUEST_TIMEOUT' || $msg eq 'GATEWAY_TIMEOUT'
        ? 'Timeout'
        : 'Request';
    my $error_params = {
        server      => $server,
        status_code => $code,
        status_msg  => $msg,
    };

    if ( $type eq 'Request' ) {
        $error_params->{content} = $content;
    }
    $self->throw( $type, $msg . ' (' . $code . ')', $error_params );
}

#===================================
sub refresh_servers {
#===================================
    my $self = shift;
    $self->clear_clients;
    return $self->SUPER::refresh_servers;
}

#===================================
sub client {
#===================================
    my $self = shift;
    my $server = shift || '';
    if ( my $client = $self->{_client}{$$}{$server} ) {
        return $client if $client->{input}{trans}->isOpen;
    }

    $self->{_client} = { $$ => {} }
        unless $self->{_client}{$$};

    my ( $host, $port ) = ( $server =~ /^(.+):(\d+)$/ );
    $self->throw( 'Param', "Couldn't understand server '$server'" )
        unless $host && $port;

    my $socket
        = ElasticSearch::Transport::ThriftBackend::Thrift::Socket->new( $host,
        $port );

    my $timeout = ( $self->timeout || 10000 ) * 1000;
    $socket->setSendTimeout($timeout);
    $socket->setRecvTimeout($timeout);

    my $transport
        = ElasticSearch::Transport::ThriftBackend::Thrift::FramedTransport
        ->new($socket);
    my $protocol
        = ElasticSearch::Transport::ThriftBackend::Thrift::BinaryProtocol
        ->new($transport);
    my $client = $self->{_client}{$$}{$server}
        = ElasticSearch::Transport::ThriftBackend::RestClient->new($protocol);

    $transport->open;

    return $client;
}

=head1 NAME

ElasticSearch::Transport::Thrift - Thrift backend

=head1 DESCRIPTION

ElasticSearch::Transport::Thrift uses the Thrift to talk to ElasticSearch
over sockets.

Although the C<thrift> interface has the right buzzwords (binary, compact,
sockets), the generated Perl code is very slow. Until that is improved, I
recommend one of the C<http> backends instead.

=head1 SYNOPSIS

    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9500',
        transport   => 'thrift',
        timeout     => '10',
    );

You need to have the C<transport-thrift> plugin installed on your
ElasticSearch server for this to work.

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport>

=item * L<ElasticSearch::Transport::HTTP>

=item * L<ElasticSearch::Transport::HTTPLite>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
