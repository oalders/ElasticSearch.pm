package ElasticSearch::Transport;

use strict;
use warnings FATAL => 'all';
use ElasticSearch::Util qw(throw parse_params);
use URI();
use JSON();

our %Transport = (
    'http'     => 'ElasticSearch::Transport::HTTP',
    'httplite' => 'ElasticSearch::Transport::HTTPLite',
    'thrift'   => 'ElasticSearch::Transport::Thrift'
);

#===================================
sub new {
#===================================
    my $class           = shift;
    my $params          = shift;
    my $transport_name  = delete $params->{transport} || 'http';
    my $transport_class = $Transport{$transport_name}
        or $class->throw(
        'Param',
        "Unknown transport '$transport_name'",
        { Available => \%Transport }
        );

    eval "require  $transport_class" or $class->throw( "Internal", $@ );

    my $self = bless { _JSON => JSON->new(), _timeout => 120 },
        $transport_class;

    my $servers = delete $params->{servers}
        or $self->throw( 'Param', 'No servers passed to new' );

    $servers = $self->servers($servers);
    $self->{_default_servers} = [@$servers];
    if ( exists $params->{timeout} ) {
        $self->timeout( delete $params->{timeout} );
    }
    $self->init($params);
    return $self;
}

#===================================
sub init { shift() }
#===================================

#===================================
sub request {
#===================================
    my $self          = shift;
    my $params        = shift;
    my $single_server = shift;

    my $json = $self->JSON;

    $params->{method} ||= 'GET';
    $params->{cmd}    ||= '/';
    $params->{qs}     ||= {};

    my $data = $params->{data};
    if ($data) {
        $data = ref $data eq 'SCALAR' ? $$data : $json->encode($data);
    }

    my $args = { %$params, data => $data };
    my $response_json;

ATTEMPT:
    while (1) {
        my $server = $single_server || $self->next_server;

        $self->log_request( $server, $args );

        $response_json = eval { $self->send_request( $server, $args ) }
            and last ATTEMPT;

        my $error = $@;
        if ( ref $error ) {
            if (  !$single_server
                && $error->isa('ElasticSearch::Error::Connection') )
            {
                warn "Error connecting to '$server' : "
                    . ( $error->{-text} || 'Unknown' ) . "\n\n";
                $self->refresh_servers;
                next ATTEMPT;
            }
            $error->{-vars}{request} = $params;
            if ( my $content = $error->{-vars}{content} ) {
                $content = $json->decode($content);
                $self->log_response($content);
                if ( $content->{error} ) {
                    $error->{-text} = $content->{error};
                    $error->{-vars}{error_trace} = $content->{error_trace}
                        if $content->{error_trace};
                    delete $error->{-vars}{content};
                }
            }
            die $error;
        }
        $self->throw( 'Request', $error, { request => $params } );
    }

    my $result = $json->decode($response_json);
    $self->log_response( $result || $response_json );
    return $result;
}

#===================================
sub refresh_servers {
#===================================
    my $self = shift;

    delete $self->{_current_server};

    my %servers = map { $_ => 1 }
        ( @{ $self->servers }, @{ $self->default_servers } );

    my @all_servers = keys %servers;
    my $protocol    = $self->protocol;

    foreach my $server (@all_servers) {
        next unless $server;

        my $nodes
            = eval { $self->request( { cmd => '/_cluster/nodes' }, $server ) }
            or next;

        my @servers = grep {$_}
            map {m{/([^]]+)}}
            map {
                   $_->{ $protocol . '_address' }
                || $_->{ $protocol . 'Address' }
                || ''
            } values %{ $nodes->{nodes} };
        next unless @servers;

        return $self->servers( \@servers );
    }

    $self->throw(
        'NoServers',
        "Could not retrieve a list of active servers:\n$@",
        { servers => \@all_servers }
    );
}

#===================================
sub next_server {
#===================================
    my $self    = shift;
    my @servers = @{ $self->servers };
    my $next    = shift @servers;
    $self->{_current_server} = { $$ => $next };
    $self->servers( @servers, $next );
    return $next;
}

#===================================
sub current_server {
#===================================
    my $self = shift;
    return $self->{_current_server}{$$} || $self->next_server;
}

#===================================
sub servers {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_servers} = ref $_[0] eq 'ARRAY' ? shift : [@_];
    }
    return $self->{_servers} || [];
}

#===================================
sub default_servers { shift->{_default_servers} }
#===================================

#===================================
sub http_uri {
#===================================
    my $self   = shift;
    my $server = shift;
    my $cmd    = shift;
    $cmd = '' unless defined $cmd;
    my $uri = URI->new( 'http://' . $server . $cmd );
    $uri->query_form(shift) if $_[0];
    return $uri->as_string;
}

#===================================
sub timeout {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_timeout} = shift;
        $self->clear_clients;
    }
    return $self->{_timeout} || 0;
}

#===================================
sub trace_calls {
#===================================
    my $self = shift;
    if (@_) {
        delete $self->{_log_fh};
        $self->{_trace_calls} = shift;
        $self->JSON->pretty( !!$self->{_trace_calls} );

    }
    return $self->{_trace_calls};
}

#===================================
sub _log_fh {
#===================================
    my $self = shift;
    unless ( exists $self->{_log_fh}{$$} ) {
        my $log_fh;
        if ( my $file = $self->trace_calls ) {
            $file = $file eq 1 ? '&STDERR' : "$file.$$";
            open $log_fh, ">>$file"
                or $self->throw( 'Internal',
                "Couldn't open '$file' for trace logging: $!" );
            binmode( $log_fh, ':utf8' );
            $log_fh->autoflush(1);
        }
        $self->{_log_fh}{$$} = $log_fh;
    }
    return $self->{_log_fh}{$$};
}

#===================================
sub log_request {
#===================================
    my $self   = shift;
    my $log    = $self->_log_fh or return;
    my $server = shift;
    my $params = shift;

    my $data = $params->{data};
    if ( defined $data ) {
        $data =~ s/'/\\u0027/g;
        $data = " -d '\n${data}'";
    }
    else {
        $data = '';
    }

    printf $log (
        "# [%s] Protocol: %s, Server: %s\n",
        scalar localtime(),
        $self->protocol, ${server}
    );
    my $uri = $self->http_uri( '127.0.0.1:9200', @{$params}{ 'cmd', 'qs' } );

    my $method = $params->{method};
    print $log "curl -X$method '$uri' ${data}\n\n";
}

#===================================
sub log_response {
#===================================
    my $self    = shift;
    my $log     = $self->_log_fh or return;
    my $content = shift;
    my $out     = ref $content ? $self->JSON->encode($content) : $content;
    my @lines   = split /\n/, $out;
    printf $log ( "# [%s] Response:\n", scalar localtime() );
    while (@lines) {
        my $line = shift @lines;
        if ( length $line > 65 ) {
            my ($spaces) = ( $line =~ /^(?:> )?(\s*)/ );
            $spaces = substr( $spaces, 0, 20 ) if length $spaces > 20;
            unshift @lines, '> ' . $spaces . substr( $line, 65 );
            $line = substr $line, 0, 65;
        }
        print $log "# $line\n";
    }
    print $log "\n";
}

#===================================
sub protocol {
#===================================
    my $self = shift;
    $self->throw( 'Internal',
        'protocol() must be subclassed in class ' . ( ref $self || $self ) );
}

#===================================
sub send_request {
#===================================
    my $self = shift;
    $self->throw( 'Internal',
        'send_request() must be subclassed in class '
            . ( ref $self || $self ) );
}

#===================================
sub client {
#===================================
    my $self = shift;
    $self->throw( 'Internal',
        'client() must be subclassed in class ' . ( ref $self || $self ) );
}

#===================================
sub clear_clients {
#===================================
    my $self = shift;
    delete $self->{_client};
}

#===================================
sub JSON { shift()->{_JSON} }
#===================================

#===================================
sub register {
#===================================
    my $class = shift;
    my $name  = shift
        || $class->throw( 'Param',
        'No transport name passed to register_transport()' );
    my $module = shift
        || $class->throw( 'Param',
        'No module name passed to register_transport()' );
    return $Transport{$name} = $module;
}

=head1 NAME

ElasticSearch::Transport - Base class for communicating with ElasticSearch

=head1 DESCRIPTION

ElasticSearch::Transport is a base class for the modules which communicate
with the ElasticSearch server.

It handles failover to the next node in case the current node closes the
connection. All requests are round-robin'ed to all live servers.

Currently, the available backends are:

=over

=item * C<http> (default)

Uses L<LWP> to communicate using HTTP. See L<ElasticSearch::Transport::HTTP>

=item * C<httplite>

Uses L<HTTP::Lite> to communicate using HTTP.
See L<ElasticSearch::Transport::HTTPLite>

=item * C<thrift>

Uses C<thrift>  to communicate using a compact binary protocol over sockets.
See L<ElasticSearch::Transport::Thrift>. You need to have the
C<transport-thrift> plugin installed on your ElasticSearch server for this
to work.

=back

You shouldn't need to talk to the transport modules directly - everything
happens via the main L<ElasticSearch> class.

=cut

=head1 SYNOPSIS


    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        transport   => 'httplite',
        timeout     => '10',
    );

    my $t = $e->transport;

    $t->protocol                    # eg 'http'
    $t->next_server                 # next node to use
    $t->current_server              # eg '127.0.0.1:9200' ie last used node
    $t->default_servers             # seed servers passed in to new()

    $t->servers                     # eg ['192.168.1.1:9200','192.168.1.2:9200']
    $t->servers(@servers);          # set new 'live' list

    $t->refresh_servers             # refresh list of live nodes

    $t->clear_clients               # clear all open clients

    $t->register('foo',$class)      # register new Transport backend

=head1 WHICH TRANSPORT SHOULD YOU USE

Although the C<thrift> interface has the right buzzwords (binary, compact,
sockets), the generated Perl code is very slow. Until that is improved, I
recommend one of the C<http> backends instead.

The C<httplite> backend is about 30% faster than the default C<http> backend,
and will probably become the default after more testing in production.

Note: my experience with L<HTTP::Lite> so far has been flawless - I'm just
being cautious.

See also: L<http://www.elasticsearch.com/docs/elasticsearch/modules/http>
and L<http://www.elasticsearch.com/docs/elasticsearch/modules/thrift>

=head1 SUBCLASSING TRANSPORT

If you want to add a new transport backend, then these are the methods
that you should subclass:

=head2 C<init()>

    $t->init($params)

Currently a no-op. Receives a HASH ref with the parameters passed in to
C<new()>, less C<servers>, C<transport> and C<timeout>.

Any parameters specific to your module should be deleted from C<$params>

=head2 C<send_request()>

    $json = $t->send_request($server,$params)

    where $params = {
        method  => 'GET',
        cmd     => '/_cluster',
        qs      => { pretty => 1 },
        data    => '{ "foo": "bar"}',
    }

This must be overridden in the subclass - it is the method called to
actually talk to the server.

See L<ElasticSearch::Transport::HTTP> for an example implementation.

=head2 C<protocol()>

    $t->protocol

This must return the protocol in use, eg C<"http"> or C<"thrift">. It is
used to extract the list of bound addresses from ElasticSearch, eg
C<http_address> or C<thrift_address>.

=head2 C<client()>

    $client = $t->client($server)

Returns the client object used in L</"send_request()">. The server param
will look like C<"192.168.5.1:9200">. It should store its clients in a PID
specific slot in C<< $t->{_client} >> as C<clear_clients()> deletes
this key.

See L<ElasticSearch::Transport::HTTP/"client()"> and
L<ElasticSearch::Transport::Thrift/"client()">
for an example implementation.

=head1 Registering your Transport backend

You can register your Transport backend as follows:

    BEGIN {
        ElasticSearch::Transport->register('mytransport',__PACKAGE__);
    }

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport::HTTP>

=item * L<ElasticSearch::Transport::HTTPLite>

=item * L<ElasticSearch::Transport::Thrift>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
